;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'fifo/engine)

(use-module '{fifo varconfig mttools stringfmts reflection logger})
(use-module '{storage/flex})

(define %loglevel %notice%)

(module-export! '{fifo/engine engine/getopt})

(define log-frequency 60)
(varconfig! engine:logfreq log-frequency)

(define check-frequency 300)
(varconfig! engine:checkfreq check-frequency)

(define fifo-condvar (within-module 'fifo fifo-condvar))

;;; This is a task loop engine where a task applies a function to a
;;;  bunch of data items organized into batches.

;;; Currently, this engine supports thread parallelism but not job
;;; (process) parallelism. There are three levels:

;;; 1. Task (outermost, cross-process)
;;; 2. Loop (single process, single invocation of fifo/engine, often one invocation per process)
;;; 3. Batch (single thread, set of items)

;;; While running, a table (slotmap) is associated with each of the
;;; levels, called *state*, *loop-state*, and *batch_state*
;;; respectively.


;;; The engine uses a FIFO of batches and a pool of threads pulling
;;; batches from the FIFO and processing them. For a single batch,
;;; there can be serveral phases (this is implemented by ENGINE-THREADFN).

#|Markdown

1. BEFOREFN is called on the batch, the batch-state, the loop-state, and the
task state)

2. PROCESSFN is the item function itself. It is called on each element
of the batch. If it takes two arguments, it is called on the item and
the batch state. If it takes 4 arguments, it is called on the item,
batch state, loop state, and task state.

3. AFTERFN is called on the batch, the batch-state, the loop-state, and the
task state)

4. The 'loop state' is then 'bumped'. This updates the total
'count' (number of items processed), total 'batches' (number of
batches processed), and 'vtime' (how much realtime this thread spent
on the batch).

5. LOGFN is then called on:
* the items processed;
* the clock time spent on PROCESS
* the clock time spent on BEFORE+PROCESS+AFTER
* the batch state
* the loop state
* the task state

5. The STOPFNs are then called and if any return non-false, the loop
'stops'. This means that the current thread stops processing batches
and all of the other threads stop when they're done with their current
batch.

6. If the loop hasn't stopped, the loop's *monitors* are called. A
monitor is either a function or a pair of functions. In the first
case, the function is simply called on the loop-state; in the second,
the CAR is called on the loop-state and the CDR is called if the CAR
returns #t.

The monitors can stop the loop by storing a value in the 'stopped
slot of the loop state.

|#

;;; These functions divide big sets (choice or vector) of items into smaller batches.
;;; The batches (except for the last one) are all some multiple of
;;; *batchsize* items; the multiple is in the range [1,*batchrange*].
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

(define (batchup-vector items batchsize batchrange)
  (let ((n (length items))
	(batchlist '())
	(start 0))
    (while (< start n)
      (let ((step (* (1+ (random batchrange)) batchsize)))
	(set! batchlist (cons (slice items start (+ start step)) batchlist))
	(set! start (+ start step))))
    (reverse (->vector batchlist))))

;;; We make this handler unique right now, but it could be engine specific.
(define-init bump-loop-state
  (slambda (loop-state (count #f) (proc-time #f) (thread-time #f))
    (store! loop-state 'batches (1+ (getopt loop-state 'batches 0)))
    (when count
      (store! loop-state 'count 
	      (+ (getopt loop-state 'count 0) count)))
    (when proc-time
      (store! loop-state 'vproctime 
	      (+ (getopt loop-state 'vproctime 0.0) proc-time)))
    (when thread-time
      (store! loop-state 'vtime 
	      (+ (getopt loop-state 'vtime 0.0) thread-time)))))

(defambda (engine/getopt loop-state optname (default #f))
  (getopt loop-state optname
	  (getopt (getopt loop-state 'state) optname
		  (getopt loop-state 'opts) optname
		  default)))

(define (engine-threadfn iterfn fifo opts loop-state state
			 beforefn afterfn
			 monitors
			 stopfn)
  (let* ((batch (fifo/pop fifo))
	 (batch-state `#[loop ,loop-state started ,(elapsed-time) batchno 1])
	 (start (get batch-state 'started))
	 (proc-time #f)
	 (batchno 1))
    (while (and (exists? batch) batch)
      (loginfo |GotBatch| "Processing batch of " ($num (choice-size batch)) " items")
      (when (and (exists? beforefn) beforefn)
	(beforefn (qc batch) batch-state loop-state state))
      (set! proc-time (elapsed-time))
      (if (getopt opts 'ndcall #f)
	  (if (non-deterministic? iterfn)
	      (cond ((= (procedure-arity iterfn) 1) (iterfn batch))
		    ((= (procedure-arity iterfn) 2) (iterfn batch batch-state))
		    ((= (procedure-arity iterfn) 4) (iterfn batch batch-state loop-state state))
		    (else (iterfn batch)))
	      (cond ((= (procedure-arity iterfn) 1) (iterfn (qc batch)))
		    ((= (procedure-arity iterfn) 2) (iterfn (qc batch) batch-state))
		    ((= (procedure-arity iterfn) 4) (iterfn (qc batch) batch-state loop-state state))
		    (else (iterfn (qc batch)))))
	  (cond ((= (procedure-arity iterfn) 1)
		 (do-choices (item batch) (iterfn item)))
		((= (procedure-arity iterfn) 2)
		 (do-choices (item batch) (iterfn item batch-state)))
		((= (procedure-arity iterfn) 4)
		 (do-choices (item batch) (iterfn item batch-state loop-state state)))
		(else (do-choices (item batch) (iterfn item)))))
      (set! proc-time (elapsed-time proc-time))
      (when (and  (exists? afterfn) afterfn)
	(afterfn (qc batch) batch-state loop-state state))
      (bump-loop-state loop-state (choice-size batch) proc-time (elapsed-time start))
      (when (dolog? fifo loop-state)
	(engine-logger (qc batch) proc-time (elapsed-time start)
		       batch-state loop-state state))
      ;; Free some stuff up, maybe
      (set! batch {})
      (let ((stopval (or (test loop-state 'stopped)
			 (and (exists? stopfn) stopfn (exists stopfn loop-state)))))
	(cond ((and (exists? stopval) stopval)
	       (store! loop-state 'stopped (timestamp))
	       (store! loop-state 'stopval stopval))
	      (monitors
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
		       (else (logwarn |BadMonitor| monitor))))))
	(cond ((getopt loop-state 'stopped)
	       (set! batch {}))
	      (else
	       (when (or (test loop-state 'checknow) (docheck? fifo loop-state))
		 (thread/wait! (thread/call engine/checkpoint fifo loop-state)))
	       (set! batch (fifo/pop fifo))
	       (set! batchno (1+ batchno))
	       (set! start (elapsed-time))
	       (set! batch-state
		 `#[loop ,loop-state started ,start batchno ,batchno])))))))

(defambda (fifo/engine fcn items (opts #f))
  (let* ((batchsize (getopt opts 'batchsize (config 'batchsize 1024)))
	 (nthreads (mt/threadcount (getopt opts 'nthreads (config 'nthreads #t))))
	 (vector-items  (and (singleton? items) (vector? items)))
	 (spacing (getopt opts 'spacing 
			  (and nthreads (> nthreads 1)
			       (/~ nthreads (ilog nthreads)))))
	 (batches (if (= batchsize 1)
		      (if vector-items items (choice->vector items))
		      (if vector-items
			  (batchup-vector items batchsize nthreads)
			  (batchup items batchsize nthreads))))
	 (n-items (if vector-items (length items) (choice-size items)))
	 (rthreads (if (and nthreads (> nthreads (length batches))) (length batches) nthreads))
	 (fifo (fifo/make batches `#[fillfn ,fifo/exhausted!]))
	 (before (getopt opts 'before #f))
	 (after (getopt opts 'after #f))
	 (stop (getopt opts 'stopfn #f))
	 (state (getopt opts 'state
			(and (getopt opts 'statefile)
			     (if (file-exists? (getopt opts 'statefile))
				 (file->dtype (getopt opts 'statefile))
				 #[]))))
	 (logfns (getopt opts 'logfns {}))
	 (checkstate (getopt opts 'checkstate {}))
	 (loop-state (frame-create #f
		       'fifo fifo
		       'started (elapsed-time)
		       'total n-items
		       'n-batches (length batches)
		       'logfreq (getopt opts 'logfreq log-frequency)
		       'checkfreq (getopt opts 'checkfreq check-frequency)
		       'monitors (getopt opts 'monitors {})
		       'logfns logfns
		       'checkstate checkstate
		       'state state
		       'opts opts))
	 (threads {})
	 (count 0))
    
    (do-choices fcn
      (unless (and (applicable? fcn) (overlaps? (procedure-arity fcn) {1 2 4}))
	(irritant fcn |ENGINE/InvalidLoopFn| fifo/engine)))
    (when (and (exists? before) before)
      (do-choices before
	(when before
	  (unless (and (applicable? before) (= (procedure-arity before) 4))
	    (irritant before |ENGINE/InvalidBeforeFn| fifo/engine)))))
    (when (and (exists? after) after)
      (do-choices after
	(unless (and (applicable? after) (= (procedure-arity after) 4))
	  (irritant after |ENGINE/InvalidAfterFn| fifo/engine))))

    (when (and (exists? logfns) logfns)
      (do-choices (logfn (difference logfns #t))
	(unless (and (applicable? logfn) (overlaps? (procedure-arity logfn) {1 3 6}))
	  (irritant logfn |ENGINE/InvalidLogfn| fifo/engine))))

    (lognotice |Engine| 
      "Processing " ($count n-items) " items "
      (when (and batchsize (> batchsize 1))
	(printout "in " ($count (length batches)) " batches "))
      "using " rthreads " threads with " fcn)

    (dotimes (i rthreads)
      (set+! threads 
	(thread/call engine-threadfn 
	    fcn fifo opts 
	    loop-state state 
	    (qc before) (qc after)
	    (getopt opts 'monitors)
	    stop))
      (when spacing (sleep spacing)))

    (thread/wait threads)
    
    (if (getopt opts 'finalcheck)
	(begin
	  (lognotice |Engine| "Doing final checkpoint for FIFO/ENGINE")
	  (engine/checkpoint fifo loop-state)
	  (commit))
	(lognotice |Engine| "Skipping final checkpoint for FIFO/ENGINE call"))))

;;;; Logging

(define (dolog? fifo loop-state (freq))
  (default! freq (try (get loop-state 'logfreq) #f))
  (with-lock (fifo-condvar fifo)
    (cond ((not freq) #t)
	  ((not (getopt loop-state 'lastlog))
	   (store! loop-state 'lastlog (elapsed-time))
	   ;; Don't do the first log report because it's almost certainly short
	   #f)
	  ((> (elapsed-time (getopt loop-state 'lastlog)) freq)
	   (store! loop-state 'lastlog (elapsed-time))
	   #t)
	  (else #f))))

(define (engine-logger batch proc-time time batch-state loop-state state)
  (let ((logfns (get loop-state 'logfns)))
    (when (overlaps? #t logfns)
      (engine/log (qc batch) proc-time time
		       batch-state loop-state state))
    (do-choices (logfn (difference logfns #t #f))
      (cond ((not (applicable? logfn))
	     (logwarn |Engine/BadLogFn| 
	       "Couldn't apply the log function " logfn)
	     (drop! loop-state 'logfns logfn))
	    ((= (procedure-arity logfn) 1)
	     (logfn loop-state))
	    ((= (procedure-arity logfn) 3)
	     (logfn batch-state loop-state state))
	    ((= (procedure-arity logfn) 6)
	     (logfn (qc batch) proc-time time batch-state loop-state state))
	    (else
	     (logwarn |Engine/BadLogFn| 
	       "Couldn't use the log function " logfn)
	     (drop! loop-state 'logfns logfn))))))

;;; The default log function
(define (engine/log batch proctime time batch-state loop-state state)
  (let* ((count (get loop-state 'count))
	 (loopmax (getopt loop-state 'total))
	 (total (getopt state 'total))
	 (taskmax (getopt state 'total))
	 (rate (/~ (get loop-state 'count) (elapsed-time (get loop-state 'started))))
	 (%loglevel (getopt loop-state '%loglevel %loglevel))
	 (fifo (getopt loop-state 'fifo)))
    (loginfo |Batch/Progress| 
      (when (testopt (fifo-opts fifo) 'static)
	(printout "(" (show% (fifo/load fifo) (fifo-size fifo) 2)") "))
      "Processed " ($num (choice-size batch)) " items in " 
      (secs->string (elapsed-time (get batch-state 'started)) 1) " or ~"
      ($num (->exact (/~ (choice-size batch) (elapsed-time (get batch-state 'started))) 0))
      " items/second for this batch and thread.")
    (lognotice |Engine/Log| 
      "Processed " ($num (get loop-state 'count)) " items" 
      (when loopmax
	(printout " (" (show% (get loop-state 'count) loopmax) " of "
	  ($num loopmax) ")"))
      " in " (secs->string (elapsed-time (get loop-state 'started)) 1) 
      (cond (loopmax
	     (let* ((togo (- loopmax count))
		    (timeleft (/~ togo rate))
		    (finished (timestamp+ timeleft))
		    (timetotal (/~ loopmax rate)))
	       (printout "\nAt " ($num (->exact rate 0)) " items/sec, "
		 "the loop's " ($num loopmax) " items should be finished in "
		 "~" (secs->string timeleft 1) " (~"
		 (get finished 'timestring) " "
		 (cond ((equal? (get (timestamp) 'datestring) (get finished 'datestring)))
		       ((< (difftime finished) (* 24 3600)) "tomorrow")
		       ((< (difftime finished) (* 24 4 3600)) (get finished 'weekday-long))
		       (else (get finished 'rfc822date)))
		 ") after " (secs->string timetotal 1))))
	    (else (printout ", averaging " 
		    ($num (->exact rate 0)) " items/second in this loop. "))))))

;;; Checkpointing

(define (docheck? fifo loop-state (freq) (fns))
  (default! freq (try (get loop-state 'checkfreq) #f))
  (and (exists? (get loop-state 'checkstate))
       (get loop-state 'checkstate)
       (with-lock (fifo-condvar fifo)
	 (cond ((not freq) #t)
	       ((not (getopt loop-state 'lastcheck))
		(store! loop-state 'lastcheck (elapsed-time))
		;; Don't do the first log report because it's almost certainly short
		#f)
	       ((> (elapsed-time (getopt loop-state 'lastcheck)) freq)
		(store! loop-state 'lastcheck (elapsed-time))
		#t)
	       (else #f)))))

(define (engine/checkpoint fifo loop-state (success #f))
  (unwind-protect 
      (begin 
	(lognotice |Engine/Checkpoint| fifo)
	(fifo/pause! fifo 'readwrite)
	(flex/save! (get loop-state 'checkstate))
	(when (and (test loop-state 'state) (get loop-state 'state)
		   (testopt (get loop-state 'opts) 'statefile))
	  (dtype->file (get loop-state 'state)
		       (getopt (get loop-state 'opts) 'statefile)))
	(set! success #t))
    (begin (unless success (logwarn |Engine/Checkpoint/Failed| fifo))
      ;; This may not be the right thing
      (fifo/pause! fifo #f))))

;;;; Utility functions

(define (engine/fetchoids oids (batch-state #f) (loop-state #f) (state #f))
  (prefetch-oids! oids))
(define (engine/fetchkeys oids (batch-state #f) (loop-state #f) (state #f))
  (prefetch-keys! oids))
(define (engine/swapout args (batch-state #f) (loop-state #f) (state #f))
  (swapout args))

(module-export! '{engine/fetchoids engine/fetchkeys engine/log})

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

