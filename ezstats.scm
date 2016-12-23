;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'ezstats)

(module-export! '{mean avg stddev median 
		  arithmetic-variance variance
		  arithmetic-mean harmonic-mean}) 

;;; Most common functions

(defambda (mean x (fn #f)) 
  (if (and (singleton? x) (or (vector? x) (list? x)))
      (arithmetic-mean (if fn (map fn (->vector x)) (->vector x)))
      (arithmetic-mean (if fn (map fn (choice->vector x))
			   (choice->vector x)))))
(defambda (stddev x (fn #f))
  (if (and (singleton? x) (or (vector? x) (list? x)))
      (sqrt (arithmetic-variance (if fn (map fn x) x)))
      (sqrt (arithmetic-variance (if fn (map fn (choice->vector x))
				     (choice->vector x))))))
;;; Arithmetic functions


(define (arithmetic-mean vec (inexact #t))
  (if inexact
      (/~ (reduce + vec) (length vec))
      (/ (reduce + vec) (length vec))))
(define avg mean)

(define (arithmetic-variance vec)
  (let ((avg (mean vec)))
    (/~ (reduce + (map (lambda (x) (* (- x avg) (- x avg))) vec))
	(length vec))))
(define variance arithmetic-variance)

(define (standard-deviation vec)
  (sqrt (arithmetic-variance vec)))

;;; Utilities

(define-init >=0? (lambda (x) (>= x 0)))
(define-init <=0? (lambda (x) (<= x 0)))

;;; Other means

(define (geometric-mean vec (weights #f) (inexact #f))
  (if weights
      (if (= (length weights) (length vec))
	  (geometric-mean (map * weights vec) #f inexact)
	  (irritant weights |Vector/WeightsMismatch|
		    "The number of weights " (length weights) " "
		    "is not the same as the the length "
		    "of the input vector " (length vec)))
      (if (or (every? >=0? vec) (every? <=0? vec))
	  (let* ((len (length vec))
		 (product (reduce * vec))
		 (root (pow product (/~ len))))
	    (if (or inexact (inexact? product))
		(* 1.0 root)
		(let ((exact-root (->exact root))
		      (prod 1))
		  (dotimes (i len) (set! prod (* prod exact-root)))
		  (if (= prod product)
		      exact-root
		      root))))
	  (irritant vec |MixedNegativePositiveElements|))))

(define (harmonic-mean vec (weights #f) (drop-zeros #f) (inexact #f))
  (if (some? zero? vec)
      (if drop-zeros 
	  (harmonic-mean (remove-if zero? vec) weights)
	  (irritant vec |ZeroValuedElements|))
      (if weights
	  (if (= (length weights) (length vec))
	      (if inexact
		  (/~ (reduce + weights) (reduce + (map /~ weights vec)))
		  (/ (reduce + weights) (reduce + (map / weights vec))))
	      (irritant weights |Vector/WeightsMismatch|
			"The number of weights " (length weights) " "
			"is not the same as the the length "
			"of the input vector " (length vec)))
	  (if inexact
	      (/~ (length vec) (reduce + (map /~ vec)))
	      (/ (length vec) (reduce + (map / vec)))))))

(define (median vec)
  (let* ((vec (sortvec vec)) 
	 (len (length vec))
	 (middle (quotient len 2)))
    (if (zero? (remainder len 2))
	(/~ (+ (elt vec middle) (elt vec (1+ middle))) 2)
	(elt vec middle))))

