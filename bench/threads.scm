;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'bench/threads)

(use-module '{logger varconfig mttools optimize stringfmts})

(module-export! '{tbench})

(define (tbench nthreads limit proc per-thread . args)
  (let ((end (if (timestamp? limit) limit 
		 (if (and (number? limit) (> limit 0))
		     (timestamp+ limit)
		     (irritant limit |Not a time limit|))))
	(threadcount (mt/threadcount nthreads))
	;; Run these once here to catch any superficial bugs before
	;; the benchmark begins
	(looptest (apply proc (and per-thread (per-thread)) args)))
    (when (config 'optimize #f)
      (optimize-procedure! proc)) ;; { loopfn} Can't optimize a lexically bound procedure
    (let ((loopfn (lambda (threadno thread-data (count 0))
		    (until (past? end)
		      (apply proc thread-data args)
		      (set! count (1+ count)))
		    count))
	  (before (rusage))
	  (thread-results #f))
      (logwarn |ThreadBenchStart| 
	"With " (or threadcount "no") " threads for " (secs->string (difftime end))
	" load=" (get before 'load) " mem=" (get before 'memusage))
      (if threadcount
	  (let ((threads {}))
	    (dotimes (i threadcount)
	      (set+! threads (threadcall loopfn i (and per-thread (per-thread)))))
	    (threadjoin threads)
	    (set! thread-results (map thread/result (choice->vector threads))))
	  (set! thread-results (vector (loopfn #f (and per-thread (per-thread))))))
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
	  (logwarn |CPU%|
	    (show% cputime clocktime) 
	    (when (and nthreads (> nthreads 1))
	      (printout " (" 
		(show% (/ cputime threadcount) clocktime)
		" * " nthreads ") "))
	    " memdiff=" (bytesize memdiff)))))))

(define (print-slots table slots (width 3) (count 0))
  (doseq (slot slots)
    (when (test table slot)
      (set! count (1+ count))
      (when (zero? (remainder count width)) (printout "\n   "))
      (printout (downcase slot) "=" (get table slot) " "))))

(define (bytesize bytes)
  (if (<= bytes 4096)
      (printout bytes " bytes")
      (if (< bytes (* 1024 1024))
	  (printout (/~ bytes 1024) "KB")
	  (printout (printnum (/~ bytes (* 1024 1024)) 2)
	    "MB"))))
