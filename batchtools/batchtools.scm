;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2016-2017 beingmeta, inc.  All rights reserved.

(in-module 'batchtools)

(use-module '{logger stringfmts varconfig slotindex})

(define start-time (elapsed-time))

(define-init maxtime #f)
(varconfig! maxtime maxtime config:interval)

(define-init maxmem (->exact (* 0.8 (physmem))))
(varconfig! maxmem maxmem config:bytes)

(define-init maxvmem (physmem))
(varconfig! maxvmem maxvmem config:bytes)

(define-init maxload #f)
(varconfig! maxload maxload)

(define-init maxcount #f)
(varconfig! maxcount maxcount)

(define-init lastlog (elapsed-time))
(define-init log-frequency 60)
(varconfig! logfreq log-frequency config:interval)

(define-init saving #f)
(define-init last-save (elapsed-time))
(define-init save-frequency #f)
(varconfig! savefreq save-frequency config:interval)

(define-init pre-save #f)
(varconfig! PRESAVE pre-save)
(define-init post-save #f)
(varconfig! POSTSAVE post-save)

(define %nosusbt '{pre-save post-save})

(define task-state #[])
(define init-state #f)

(defambda (nstore! table slot value)
  (unless (fail? value) (store! table slot value)))

(defambda (batch/read-state file . defaults)
  (let* ((init-state (if (and file (file-exists? file))
			 (file->dtype file)
			 (apply frame-create #f defaults)))
	 (state (deep-copy init-state)))
    (store! state 'init init-state)
    (when file (store! state 'statefile file))
    (nstore! state 'pools (use-pool (get state 'pools)))
    (nstore! state 'indices (open-index (get state 'indices)))
    (nstore! state 'objects
	     (for-choices (obj (get state 'objects))
	       (if (and (string? obj) (file-exists? obj))
		   (cons obj (file->dtype obj))
		   (if (and (pair? obj) (string? (car obj)))
		       (if (file-exists? (car obj))
			   (cons (car obj) (file->dtype (car obj)))
			   (begin (dtype->file (cdr obj) (car obj))
			     obj))
		       (irritant obj |BadObjectStateReference|)))))
    (nstore! state 'slotindex
	     (slotindex/init (get state 'slotindex)))
    (unless (test state 'begun) (store! state 'begun (timestamp)))
    (store! state 'process-count (1+ (try (get state 'process-count) 0)))
    state))

(define (batch/start! (state task-state) . defaults)
  (when (string? state) 
    (set! state (apply batch/read-state state defaults)))
  (set! start-time (elapsed-time))
  (store! state 'cycle-count (1+ (try (get state 'cycle-count) 0)))
  (unless init-state (set! init-state (deep-copy state)))
  (unless (test state 'begun)
    (store! state 'begun (gmtimestamp 'seconds))))

(module-export! '{batch/read-state batch/start!})

;;; Are we done yet?

(define (batch/finished? (state task-state))
  (or finished
      (exists batch/threshtest "Time" (elapsed-time start) maxtime secs->string)
      (exists batch/threshtest "Memory" (memusage) maxmem $bytes)
      (exists batch/threshtest "VMEM" (vmemusage) maxvmem $bytes)
      (exists batch/threshtest "LOAD" (get (rusage) 'load) maxload)
      (exists batch/threshtest "Progress" (get state 'progress) maxcount)
      (exists test-limit state tasklimits)))

(define-init finished #f)
(define (batch/threshtest condition cur max (format #f))
  (or finished
      (and cur max (> cur max) 
	   (begin
	     (logwarn |Finishing| "Due to " condition ": " 
		      (if format (format cur) cur) " > "
		      (if format (format max) max))
	     (set! finished #t)
	     #t))))

(define tasklimits {})
(varconfig! tasklimit tasklimits)

(define (test-limit state limit)
  (and (test state (car limit))
       (number? (cdr limit))
       (number? (get state (car limit)))
       (> (get state (car limit)) (cdr limit))))

(module-export! '{batch/finished? batch/threshtest})

;;; Regular checkins

(defslambda (batch/checkin delta (state task-state))
  (if (table? delta)
      (do-choices (key (getkeys delta))
	(let ((dv (get delta key)))
	  (if (number? dv)
	      (store! state key (+ dv (try (get state key) 0)))
	      (add! state key dv))))
      (if (number? delta)
	  (store! state 'progress 
		  (+ delta (try (get state 'progress) 0)))
	  (add! state 'checkins delta)))
  (unless finished
    (when (batch/finished? task-state)
      (set! finished #t))
    (when (and logfreq (> (elapsed-time last-log) logfreq))
      (batch/log! state))
    (when (and savefreq (not saving) (not (zero? savefreq))
	       (> (elapsed-time last-save) savefreq) 
	       (getsavelock))
      (batch/save! state))))

(module-export! '{batch/checkin})

;;; Checkpointing

(defslambda (batch/save! (state task-state))
  (and saving
       (if (and pre-save (not (pre-save)))
	   (begin 
	     (logwarn |BatchSaveAbort| "PRESAVE function aborted save")
	     (set! saving #f))
	   (let ((threads
		  (choice
		   (for-choices (pool (get state 'pools))
		     (threadcall commit-pool pool))
		   (for-choices (index (get state 'indices))
		     (threadcall commit index))
		   (for-choices (file.object (get state 'objects))
		     (threadcall dtype->file 
				 (deep-copy (cdr file.object))
				 (car file.object))))))
	     (lognotice |BatchSave| 
	       "Saving state using " (choice-size threads) " threads")
	     (thread/join threads)
	     (lognotice |BatchSave| 
	       "Writing out processing state to " (get state 'statefile))
	     (when post-save (post-save state-copy))
	     (let ((state-copy (deep-copy state)))
	       (store! state-copy 'pools
		       (pool-source (get state-copy 'pools)))
	       (store! state-copy 'indices
		       (index-source (get state-copy 'indices)))
	       (store! state-copy 'objects (car (get state-copy 'objects)))
	       (store! state-copy 'elapsed 
		       (+ (elapsed-time start-time) 
			  (try (get state 'elapsed) 0)))
	       (dtype->file state-copy (get state-copy 'statefile)))))))

(defslambda (getsavelock)
  (and (not saving) 
       (begin (set! saving (threadid)) #t)))

(define (batch/savelock)
  (and (not saving) (getsavelock)))

(module-export! '{batch/save! batch/savelock})

;;; Logging

(define (batch/log! (state task-state))
  (lognotice |#| (make-string 120 #\#))
  (lognotice |Batch|
    "After " (secs->string (elapsed-time start-time)) ", " 
    (do-choices (log (get state 'logging))
      (if (symbol? log)
	  (when (test state log)
	    (printout " " log "=" (get state log)))
	  (if (and (pair? log) (symbol? (car log))
		   (test state (car log))
		   (applicable? (cdr log)))
	      (printout " " (car log) "=" 
		((cdr log) (get state (car log))))))))
  (when (test state 'rates)
    (let ((elapsed (elapsed-time start-time)))
      (lognotice |Progress|
	(do-choices (rate (get state 'rates) i)
	  (when (test state rate)
	    (printout (printnum (/ (get state rate) elapsed) 1)
	      (downcase rate) "/sec;"))))))
  (let ((u (rusage)))
    (lognotice |Resources|
      "CPU=" (printnum (get u 'cpu%) 2) 
      " (" (printnum (/~ (get u 'cpu%) (get u 'ncpus)) 2)
      " * " (get u 'ncpus) " cpus) "
      (when (test state 'nthreads)
	(printout " (" (printnum (/~ (get u 'cpu%) (get state 'nthreads)) 2) " * "
	  (get state 'nthreads) " threads) ")))
    (lognotice |Resources|
      "MEM=" ($bytes (get u 'memusage)) 
      ", VMEM=" ($bytes (get u 'vmemusage))
      (when maxmem (printout ", MAXMEM=" ($bytes maxmem)))
      (when maxvmem (printout ", MAXMEM=" ($bytes maxvmem)))
      ", PHYSMEM=" ($bytes (get u 'physical-memory))))
  (lognotice |#| (make-string 120 #\-)))

(module-export! '{batch/log!})
