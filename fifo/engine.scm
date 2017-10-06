;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'fifo/engine)

(use-module '{fifo mttools stringfmts logger})

(define %loglevel %notice%)

(module-export! '{fifo/engine})

;;; This divides a big set (choice) of items into smaller buckets.
;;; The buckets (except for the last one) are all some multiple of
;;; *batchsize* items; the multiple is in the range [1,N].
;;; It returns a vector of those batches.
(defambda (batchup items batchsize batchrange)
  (let ((n (choice-size items))
	(batchlist '())
	(start 0))
    (while (< start n)
      (let ((step (* (1+ (random batchrange)) batchsize)))
	(set! batchlist (cons (qc (pick-n items step start)) batchlist))
	(set! start (+ start step))))
    (reverse (->vector batchlist))))

;;; We make this handler unique right now, but it could be engine specific.
(define-init bump-loop-state
  (slambda (loop-state (count #f) (thread-time #f))
    (when thread-time
      (store! loop-state 'count 
	      (+ (getopt loop-state 'count 0) count)))
    (when thread-time
      (store! loop-state 'vtime 
	      (+ (getopt loop-state 'vtime 0.0) thread-time)))))

(define (batch-step fcn fifo opts loop-state state
		    beforefn afterfn progressfn stopfn)
  (let* ((batch (fifo/pop fifo))
	 (start (elapsed-time)))
    (while (and (exists? batch) batch)
      (loginfo |GotBatch| "Processing batch of " ($num (choice-size batch)) " items")
      (when beforefn (beforefn (qc batch) loop-state))
      (do-choices (item batch) (fcn item))
      (when afterfn (afterfn (qc batch) loop-state))
      (bump-loop-state loop-state (choice-size batch) (elapsed-time start))
      (when progressfn
	(progressfn (qc batch) (elapsed-time start) loop-state))
      (if (and stopfn (stopfn loop-state))
	  (set! batch {})
	  (let ((monitors (getopt opts 'monitors {})))
	    (do-choices (monitor monitors)
	      (cond ((applicable? monitor) (monitor loop-state))
		    ((and (pair? monitor) (applicable? (car monitor)))
		     (let ((testval ((car monitor) loop-state)))
		       (when testval
			 (do-choices (action (if (pair? (cdr monitor))
						 (cadr monitor)
						 (cdr monitor)))
			   (if (applicable? action)
			       (action testval loop-state)
			       (logwarn |BadMonitorAction| action))))))
		    (else (logwarn |BadMonitor| monitor))))
	    (if (getopt loop-state 'stopped)
		(set! batch {})
		(begin
		  (set! batch (fifo/pop fifo))
		  (set! start (elapsed-time)))))))))

(defambda (fifo/engine fcn items (opts #f))
  (let* ((batchsize (getopt opts 'batchsize (config 'batchsize 1024)))
	 (nthreads (mt/threadcount (getopt opts 'nthreads (config 'nthreads #t))))
	 (spacing (getopt opts 'spacing 
			  (and nthreads (> nthreads 1)
			       (/~ nthreads (ilog nthreads)))))
	 (batches (batchup items batchsize nthreads))
	 (rthreads (if (and nthreads (> nthreads (length batches))) (length batches) nthreads))
	 (fifo (fifo/make batches `#[fillfn ,fifo/exhausted!]))
	 (before (getopt opts 'before #f))
	 (after (getopt opts 'after #f))
	 (progress (getopt opts 'progress #f))
	 (stop (getopt opts 'stopfn #f))
	 (state (getopt opts 'state #f))
	 (loop-state (frame-create #f
		       'fifo fifo
		       'started (elapsed-time)
		       'total (choice-size items)
		       'n-batches (length batches)
		       'monitors (getopt opts 'monitors {})
		       'state state
		       'opts opts))
	 (threads {})
	 (count 0))
    
    (lognotice |FIFOEngine| 
      "Processing " ($count (choice-size items)) " items "
      "in " ($count (length batches)) " batches " 
      "of up to " nthreads " ❌ " batchsize " items "
      "using " rthreads " threads "
      "with\n   " fcn)
    (dotimes (i rthreads)
      (set+! threads 
	(thread/call batch-step 
	    fcn fifo opts 
	    loop-state state 
	    before after progress stop)
	(when spacing (sleep spacing))))
    (thread/wait threads)
    (if (getopt opts 'skipcommit)
	(lognotice |FIFOEngine| "Skipping final commit for FIFO/ENGINE call")
	(begin
	  (lognotice |FIFOEngine| "Doing final commit for FIFO/ENGINE")
	  (commit)))))

(define (engine/fetchoids oids (state #f))
  (prefetch-oids! oids))
(define (engine/fetchkeys oids (state #f))
  (prefetch-keys! oids))
(define (engine/swapout args (state #f))
  (swapout args))

(define (engine/poolfetch pool)
  (ambda (oids (state #f)) (pool-prefetch! pool oids)))

(define (engine/indexfetch index)
  (ambda (keys (state #f)) (prefetch-keys! index keys)))

(define (engine/savepool pool)
  (ambda (keys (loop-state #f)) (commit pool)))

(define (engine/saveindex index)
  (ambda (keys (loop-state #f)) (commit index)))

(define (engine/savetable table file)
  (ambda (keys (loop-state #f)) (dtype->file table file)))

(define (engine/interval interval)
  (slambda ((loop-state #f))
    (cond ((> (elapsed-time last) interval)
	   (set! last (elapsed-time))
	   #t)
	  (else #f))))

(define (engine/maxitems max-count)
  (slambda ((loop-state #f))
    (cond ((> (getopt loop-state 'count 0) max-count)
	   #t)
	  (else #f))))

(define (engine/memgrowth delta)
  (let ((last (memusage)))
    (slambda ((loop-state #f)) 
      (cond ((> (- (memusage) last) delta)
	     (set! last (memusage))
	     #t)
	    (else #f)))))

(define (optconf opts prop (dflt #f) (use-config))
  (default! use-config
    (getopt opts 'useconfig (not (getopt opts 'noconfig #f))))
  (if use-config
      (getopt opts prop (and use-config (config prop dflt)))
      (getopt opts prop dflt)))

(define (engine/stopfn (opts #f))
  (let ((maxtime (getopt opts 'maxtime #f))
	(maxcount (getopt opts 'maxcount #f))
	(maxmem (getopt opts 'maxmem #f))
	(maxload (getopt opts 'maxload #f)))
    (lambda (loop-state)
      (or (test loop-state 'stopped)
	  (and (or (and maxtime 
			(> (elapsed-time (get loop-state 'started))
			   maxtime))
		   (and maxcount (> (get loop-state 'count) maxcount))
		   (and maxload (> (rusage 'load) maxload)))
	       (begin (store! loop-state 'stopped)
		 #t))))))

(module-export! '{engine/interval engine/memgrowth engine/stopfn
		  engine/fetchoids engine/fetchkeys 
		  engine/poolfetch engine/indexfetch engine/swapout 
		  engine/savepool engine/saveindex engine/savetable
		  engine/maxitems})

