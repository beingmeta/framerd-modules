;;; -*- Mode: Scheme -*

(in-module 'bench)

(use-module 'optimize)

(define (bench benchmark-name benchmark-thunk)
  (let ((start (timestamp)))
    (dotimes (i 5) (benchmark-thunk))
    (let ((time (difftime (timestamp) start)))
      (lineout "Benchmark " benchmark-name
	       " took " time " seconds"))))

(module-export! '{bench})

