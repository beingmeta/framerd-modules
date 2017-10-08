;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'fifo/engine)

(use-module '{fifo varconfig mttools stringfmts reflection logger})

(define %loglevel %notice%)

(module-export! '{fifo/engine})

(define engine-log-frequency 60)
(varconfig! engine:logfreq engine-log-frequency)

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
    (store! loop-state 'batches (1+ (getopt loop-state 'batches 0)))
    (when count
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
	(if (= (procedure-arity progressfn) 1)
	    (progressfn loop-state)
	    (progressfn (qc batch) (elapsed-time start) loop-state)))
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
	 (batches (if (= batchsize 1)
		      (choice->vector items)
		      (batchup items batchsize nthreads)))
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
    
    (unless (and (applicable? fcn)
		 (= (procedure-min-arity fcn) 1))
      (irritant fcn |ENGINE/InvalidLoopFn| fifo/engine))
    (when progress
      (unless (and (applicable? progress)
		   (or (= (procedure-arity progress) 1)
		       (= (procedure-arity progress) 3)))
	(irritant progress |ENGINE/InvalidProgressFn| fifo/engine)))
    (when before
      (unless (and (applicable? before) (= (procedure-min-arity before) 2))
	(irritant before |ENGINE/InvalidBeforeFn| fifo/engine)))
    (when after
      (unless (and (applicable? after) (= (procedure-min-arity after) 2))
	(irritant before |ENGINE/InvalidBeforeFn| fifo/engine)))

    (lognotice |FIFOEngine| 
      "Processing " ($count (choice-size items)) " items "
      (when (and batchsize (> batchsize 1))
	(printout "in " ($count (length batches)) " batches " 
	  "of up to " nthreads " ❌ " batchsize " items "))
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

;;;; Utility functions

(define (engine/fetchoids oids (state #f))
  (prefetch-oids! oids))
(define (engine/fetchkeys oids (state #f))
  (prefetch-keys! oids))
(define (engine/swapout args (state #f))
  (swapout args))

(define (engine/progress loop-state)
  (lognotice |Progress| 
    "Processed " ($num (get loop-state 'count)) " items in " 
    ($num (get loop-state 'batches)) " after "
    (secs->string (elapsed-time (get loop-state 'started)))))

(module-export! '{engine/fetchoids engine/fetchkeys engine/progress})

;;;; Utility meta-functions

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

(define (engine/interval interval (last (elapsed-time)))
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

(define (engine/stopfn (opts #f))
  (let ((maxtime (getopt opts 'maxtime #f))
	(maxcount (getopt opts 'maxcount #f))
	(maxbatches (getopt opts 'maxbatches #f))
	(maxmem (getopt opts 'maxmem #f))
	(maxvmem (getopt opts 'maxvmem #f))
	(maxload (getopt opts 'maxload #f))
	(stopfile (getopt opts 'stopfile #f))
	(stopconf (getopt opts 'stopconf #f)))
    (lambda (loop-state)
      (or (test loop-state 'stopped)
	  (and (or (and maxcount (> (get loop-state 'count) maxcount))
		   (and maxbatches (> (get loop-state 'batches) maxbatches))
		   (and maxtime 
			(> (elapsed-time (get loop-state 'started))
			   maxtime))
		   (and maxload
			(if (number? maxload)
			    (> (getload) maxload)
			    (and (vector? maxload) (= (length maxload) 1)
				 (number? (elt maxload 0))
				 (every? (lambda (loadval) (> loadval (elt maxload 0)))
					 (rusage 'loadavg)))))
		   (and maxvmem (> (vmemusage) maxvmem))
		   (and maxmem (> (memusage) maxmem))
		   (and stopfile (file-exists? stopfile))
		   (and stopconf (config stopconf)))
	       (begin (store! loop-state 'stopped (timestamp))
		 #t))))))

(define (engine/callif (opts #f) call)
  (if (not (and (applicable? call) (overlaps? (procedure-arity call) {1 0})))
      (irritant call |NotApplicable| engine/callif)
      (let ((maxtime (getopt opts 'maxtime #f))
	    (maxcount (getopt opts 'maxcount #f))
	    (maxbatches (getopt opts 'maxbatches #f))
	    (maxmem (getopt opts 'maxmem #f))
	    (maxvmem (getopt opts 'maxvmem #f))
	    (filename (getopt opts 'filename #f))
	    (confname (getopt opts 'confname #f))
	    (maxload (getopt opts 'maxload #f))
	    ;; Keep at least *interval* seconds between calls
	    (interval (getopt opts 'interval 1))
	    (last-call #f))
	(let ((clear?
	       (slambda ()
		 (cond ((not interval) #t)
		       ((< (elapsed-time last-call) interval) #f)
		       (else (set! last-call (elapsed-time)) #t)))))
	  (lambda (loop-state)
	    (unless last-call
	      (set! last-call (getopt loop-state 'started (elapsed-time))))
	    (when (or (not interval) 
		      (and (> (elapsed-time last-call) interval) (clear?)))
	      (when (or (and maxcount (> (get loop-state 'count) maxcount))
			(and maxbatches (> (get loop-state 'batches) maxbatches))
			(and maxtime 
			     (> (elapsed-time (get loop-state 'started))
				maxtime))
			(and maxvmem (> (vmemusage) maxvmem))
			(and maxmem (> (memusage) maxmem))
			(and maxload
			     (if (number? maxload)
				 (> (getload) maxload)
				 (and (vector? maxload) (= (length maxload) 1)
				      (number? (elt maxload 0))
				      (every? (lambda (loadval) (> loadval (elt maxload 0)))
					      (rusage 'loadavg)))))
			(and confname (config confname))
			(and filename (file-exists? filename)))
		(if (zero? (procedure-arity call))
		    (call)
		    (call loop-state)))))))))

(module-export! '{engine/stopfn engine/callif
		  engine/interval engine/memgrowth 
		  engine/poolfetch engine/indexfetch engine/swapout 
		  engine/savepool engine/saveindex engine/savetable
		  engine/maxitems})

