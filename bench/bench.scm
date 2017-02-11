;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2016-2017 beingmeta, inc.  All rights reserved.

(in-module 'bench)

(use-module 'optimize)

(define (bench benchmark-name benchmark-thunk (repeat 5))
  (let ((start (timestamp)))
    (dotimes (i repeat) (benchmark-thunk))
    (let ((time (difftime (timestamp) start)))
      (lineout "Benchmark " benchmark-name 
	" (" repeat " cylces) took " time " seconds"))))

(module-export! '{bench})

