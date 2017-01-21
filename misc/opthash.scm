;;; -*- Mode: Scheme; -*-

;;; Functions for finding optimal table sizes

(in-module 'misc/opthash)

(use-module '{ezstats crypto mttools logger varconfig})

(module-export! '{random-object test-size sample-objects})

(define default-n-samples (* 200000))
(define default-n-objects (* 200000))

(define object-pool-size 500000)

(define object-pool {})

(define default-hashfn ptrlock)

(defslambda (init-object-pool)
  (when (fail? object-pool)
    (logwarn |GeneratingPool|
      "Generating object pool of " object-pool-size " objects")
    (let ((objects {}))
      (dotimes (i object-pool-size)
	(set+! objects (random-object)))
      (set! object-pool objects))
    (logwarn |GeneratingPool|
      "Generated object pool of " (choice-size object-pool) " objects")))

(define (sample-objects n)
  (when (fail? object-pool) (init-object-pool))
  (sample-n object-pool n))

(defambda (test-size size (hashfn default-hashfn) (n-samples default-n-samples))
  (let ((objects (sample-objects n-samples))
	(distribution (make-hashtable n-samples)))
    (do-choices (object objects)
      (hashtable-increment! distribution (hashfn object size)))
    (frame-create #f
      'test-size size 
      'sample (choice-size objects)
      'covered (choice-size (getkeys distribution))
      'mean (mean (getkeys distribution) distribution)
      'stddev (stddev (getkeys distribution) distribution)
      'max (largest (get distribution (getkeys distribution)))
      'min (smallest (get distribution (getkeys distribution))))))

(define (sqrand n) (->exact (sqrt (random (1+ (* n n))))))

(define (random-object (n (random 1000)) (r (random 3)))
  (cond ((= r 0) (cons (random 17000000) (random 21000000)))
	((= r 1) (random-packet (sqrand n)))
	(else (->vector (random-packet (sqrand n))))))














