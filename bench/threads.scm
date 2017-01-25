;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'bench/threads)

(use-module '{logger varconfig mttools stringfmts})

(module-export! '{tbench})

(define (tbench nthreads limit proc per-thread . args)
  (let ((end (if (timestamp? limit) limit 
		 (if (and (number? limit) (> limit 0))
		     (timestamp+ limit)
		     (irritant limit |Not a time limit|))))
	(threadcount (mt/threadcount nthreads))
	;; Run these once here to catch any superficial bugs before
	;; the benchmark begins
	(thread-data-test (and per-thread (per-thread)))
	(looptest (apply per-thread thread-data args))
	(threads {}))
    (let ((loopfn (lambda (threadno thread-data)
		    (until (past? end)
		      (apply proc thread-data args))))
	  (before (rusage)))
      (logwarn |ThreadBenchStart| 
	(print-slots before '#(pid load resident memusage)))
      (dotimes (i nthreads)
	(set+! threads 
	       (threadcall loopfn i (and per-thread (per-thread)))))
      (let ((after (rusage)))
	(let* ((clocktime (- (get after 'elapsed) (get before 'elapsed)))
	       (stime (- (get after 'stime) (get before 'stime)))
	       (utime (- (get after 'utime) (get before 'utime)))
	       (cputime (+ stime utime))
	       (memdiff (- (get after 'memusage) (get before 'memusage))))
	  (logwarn |ThreadBenchFinished| 
	    "Elapsed: " (secs->string clocktime) 
	    ", cputime=" (secs->string cputime)
	    ", utime=" (secs->string utime)
	    ", stime=" (secs->string stime) "\n"
	    "    load=" (get after 'load) ", avg="
	    " " (first (get after 'loadavg))  
	    " " (second (get after 'loadavg))
	    " " (third (get after 'loadavg)) "\n    "
	    (print-slots after '#(memusage resident)))
	  (logwarn |ThreadBenchSummary|
	    "CPU=" (show% cputime clocktime) 
	    (when (and nthreads (> nthreads 1))
	      (printout " (" 
		(show% (/ cputime nthreads) clocktime)
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
