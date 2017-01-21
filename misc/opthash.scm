;;; -*- Mode: Scheme; -*-

;;; Functions for finding optimal table sizes

(in-module 'misc/opthash)

(use-module '{ezstats crypto mttools logger varconfig})

(module-export! '{random-object random-cons test-size sample-objects})

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

(defambda (test-size size
		     (hashfn default-hashfn) 
		     (n-samples))
  (default! n-samples (* (ilog size) 1717))
  (let ((objects (sample-objects n-samples))
	(distribution (make-hashtable (* size 3)))
	(errors (make-hashtable (* size 3)))
	(sqerrors (make-hashtable (* size 3))))
    (do-choices (object objects)
      (hashtable-increment! distribution (hashfn object size)))
    (let* ((n-objects (choice-size objects))
	   (perfect (/~ n-objects size)))
      (do-choices (slot (getkeys distribution))
	(when (> (get distribution slot) perfect)
	  (store! errors slot (- (get distribution slot) perfect))
	  (store! sqerrors slot
		  (* (- (get distribution slot) perfect)
		     (- (get distribution slot) perfect)))))
      (frame-create #f
	0 size 'nslots size 
	1 (mean (getkeys errors) errors) 
	'sample (choice-size objects) 'perfect perfect
	'covered (choice-size (getkeys distribution))
	'mean (mean (getkeys distribution) distribution)
	'stddev (stddev (getkeys distribution) distribution)
	'meanerr (mean (getkeys errors) errors)
	'stderr (stddev (getkeys errors) errors)
	'meansqerr (mean (getkeys sqerrors) sqerrors)
	'stdsqerr (stddev (getkeys sqerrors) sqerrors)
	'max (largest (get distribution (getkeys distribution)))
	'min (smallest (get distribution (getkeys distribution)))
	'maxerr (largest (get errors (getkeys errors)))
	'maxsqerr (largest (get sqerrors (getkeys sqerrors)))))))

(define (sqrand n) (->exact (sqrt (random (1+ (* n n))))))

(define (random-cons (n (random 1000)) (r (random 3)))
  (cond ((= r 0) (cons (random 17000000) (random 21000000)))
	((= r 1) (random-packet (sqrand n)))
	(else (->vector (random-packet (sqrand n))))))

(define random-object random-cons)
(varconfig! opthash:randobj random-object)

