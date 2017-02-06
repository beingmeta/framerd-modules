;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2016-2017 beingmeta, inc.  All rights reserved.

(in-module 'batchtools)

(use-module '{logger stringfmts varconfig})

(define start-time (elapsed-time))

(define-init maxtime #f)
(varconfig! maxtime maxtime config:interval)

(define-init maxmem (->exact (* 0.8 (physmem))))
(varconfig! maxmem maxmem config:bytes)

(define-init maxvmem (physmem))
(varconfig! maxvmem maxvmem config:bytes)

(define-init maxload #f)
(varconfig! maxload maxload)

(define-init lastlog (elapsed-time))
(define-init log-frequency 60)
(varconfig! logfreq log-frequency config:interval)

(define-init last-save (elapsed-time))
(define-init save-frequency 300)
(varconfig! savefreq save-frequency config:interval)

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

(define task-state #[])
(define init-state #f)

(define (batch/finished? (state task-state))
  (or finished
      (batch/threshtest "Time" (elapsed-time start) maxtime secs->string)
      (batch/threshtest "Memory" (memusage) maxmem $bytes)
      (batch/threshtest "VMEM" (vmemusage) maxvmem $bytes)
      (batch/threshtest "LOAD" (get (rusage) 'load) maxload)
      (exists test-limit state tasklimits)))

(defslambda (batch/checkin delta (state task-state))
  (unless finished
    (do-choices (key (getkeys delta))
      (let ((dv (get delta key)))
	(if (number? dv)
	    (store! state key (+ dv (try (get state key) 0)))
	    (add! state key dv))))
    (when (batch/finished? task-state)
      (set! finished #t))
    (when (and logfreq (> (elapsed-time last-log) logfreq))
      (batch/log! state))
    (when (and savefreq (> (elapsed-time last-save) savefreq)
	       (not saving) 
	       (getsavelock))
      (batch/save! state))))

(define (batch/save! (state task-state) (threads {}))
  (set! threads
	(choice
	 (for-choices (pool (get state 'pools))
	   (threadcall commit-pool pool))
	 (for-choices (index (get state 'indices))
	   (threadcall commit-index index))
	 (for-choices (file.object (get state 'objects))
	   (threadcall dtype->file 
		       (deep-copy (cdr file.object))
		       (car file.object)))))
  (threadjoin threads)
  (let ((state-copy (deep-copy state)))
    (store! state-copy 'pools
	    (pool-source (get state-copy 'pools)))
    (store! state-copy 'indices
	    (index-source (get state-copy 'indices)))
    (store! state-copy 'objects (car (get state-copy 'objects)))
    (store! state-copy 'elapsed 
	    (+ (elapsed-time start-time) 
	       (ry (get state 'elapsed) 0)))
    (dtype->file state-copy (get state-copy 'state-file))))

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

(define (batch/start! (state task-state))
  (set! start-time (elapsed-time))
  (store! state 'ncycles (1+ (try (get state 'ncycles) 0)))
  (unless init-state (set! init-state (deep-copy state)))
  (unless (test state 'begun)
    (store! state 'begun (gmtimestamp 'seconds))))
