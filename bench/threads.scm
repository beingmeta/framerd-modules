;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'bench/threads)

(use-module '{logger varconfig mttools optimize stringfmts})

(module-export! '{tbench})

(config! 'thread:logexit #f)

(define (get-thread-state arg)
  (and arg
       (if (applicable? arg) 
	   (arg)
	   (deep-copy arg))))

(define (tbench nthreads limit thread-state proc . args)
  (let ((end (if (timestamp? limit) limit 
		 (if (and (number? limit) (> limit 0))
		     (timestamp+ limit)
		     (irritant limit |Not a time limit|))))
	(threadcount (mt/threadcount nthreads))
	;; Run these once here to catch any superficial bugs before
	;; the benchmark begins
	(thread-test-data (get-thread-state thread-state)))
    (logwarn |Testing| "Testing thread proc " proc)
    (if thread-state
	(apply proc thread-test-data args)
	(apply proc args))
    (let ((loopfn (lambda (threadno (thread-data #f) (count 0))
		    (until (past? end)
		      (if thread-state
			  (apply proc thread-data args)
			  (apply proc args))
		      (set! count (1+ count)))
		    count))
	  (before (rusage))
	  (thread-results #f))
      (logwarn |Starting| 
	"With " (or threadcount "no") " threads for "
	(secs->string (difftime end))
	": load=" (get before 'load) ", mem=" ($bytes (get before 'memusage)))
      (if threadcount
	  (let ((threads {}))
	    (dotimes (i threadcount)
	      (set+! threads 
		     (threadcall loopfn i (get-thread-state thread-state))))
	    (threadjoin threads)
	    (set! thread-results (map thread/result (choice->vector threads))))
	  (set! thread-results
		(vector (loopfn #f (get-thread-state thread-state)))))
      (let ((after (rusage)))
	(let* ((clocktime (- (get after 'clock) (get before 'clock)))
	       (stime (- (get after 'stime) (get before 'stime)))
	       (utime (- (get after 'utime) (get before 'utime)))
	       (cputime (+ stime utime))
	       (memdiff (- (get after 'memusage) (get before 'memusage)))
	       (total-calls (reduce + thread-results 0)))
	  (logwarn |Finished| 
	    (printnum total-calls) " calls in " (secs->string clocktime) " "
	    "(" (printnum (/~ total-calls clocktime) 2) " calls/sec) "
	    "using " (secs->string cputime) " cputime")
	  (logwarn |Resource| 
	    "user: " (secs->string utime) "; "
	    "system: " (secs->string stime) "; "
	    "mem: " ($bytes (get after 'memusage)))
	  (logwarn |Performance|
	    "CPU=" (show% cputime clocktime) 
	    (when (and threadcount (> threadcount 1))
	      (printout " (" 
		(show% (/ cputime threadcount) clocktime)
		" * " threadcount ") "))
	    "; memdiff=" ($bytes memdiff)))))))

(define (print-slots table slots (width 3) (count 0))
  (doseq (slot slots)
    (when (test table slot)
      (set! count (1+ count))
      (when (zero? (remainder count width)) (printout "\n   "))
      (printout (downcase slot) "=" (get table slot) " "))))
