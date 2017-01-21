;;; -*- Mode: Scheme -*-

;;; This generates data on the effectiveness of a hash function for
;;; different table sizes.

(use-module '{misc/opthash optimize mttools logger})

(optimize! 'misc/opthash)

(define %loglevel %notice%)

(define default-outfile (get-component "sizetests.dtype"))

(define (nrange bottom top)
  (let ((r {}))
    (dotimes (i (- top bottom))
      (set+! r (+ bottom i)))
    r))

(define (main (outfile default-outfile) (start 31) (end 5000))
  (when (and (number? outfile) (number? start))
    (set! end start)
    (set! start outfile)
    (set! outfile default-outfile))
  (let ((tests {}))
    (do-choices-mt (size (nrange start end))
      (let ((test (test-size size)))
	(lognotice |Result|
	  "Table size of " size 
	  " err mean=" (get test 'meanerr)
	  " err stddev=" (get test 'stderr)
	  " err max=" (get test 'maxerr)
	  " err min=" (get test 'minerr))
	(set+! tests test)))
    (dtype->file tests outfile)))

(when (config 'optimize #t)
  (optimize! '{misc/opthash mttools})
  (optimize!))

