;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'fifo/engine)

(use-module '{fifo mttools stringfmts logger})

(define %loglevel %notice%)

(module-export! '{fifo/engine})

(defambda (batchup items batchsize nthreads)
  (let ((n (choice-size items))
	(batchlist '())
	(start 0))
    (while (< start n)
      (let ((step (* (1+ (random nthreads)) batchsize)))
	(set! batchlist (cons (qc (pick-n items step start)) batchlist))
	(set! start (+ start step))))
    (reverse (->vector batchlist))))

(define (batch-step fcn fifo opts loop-state state
		    beforefn afterfn progressfn)
  (let* ((batch (fifo/pop fifo))
	 (start (elapsed-time)))
    (while (and (exists? batch) batch)
      (loginfo |GotBatch| "Processing batch of " ($num (choice-size batch)) " items")
      (when beforefn
	(if state (beforefn (qc batch) state)  (beforefn (qc batch))))
      (do-choices (item batch) (fcn item))
      (when afterfn
	(if state (afterfn (qc batch) state) (afterfn (qc batch))))
      (when progressfn
	(if state
	    (progressfn (choice-size batch) (elapsed-time start) state)
	    (progressfn (choice-size batch) (elapsed-time start))))
      (set! batch (fifo/pop fifo))
      (set! start (elapsed-time)))))

(defambda (fifo/engine fcn items (opts #f))
  (let* ((batchsize (getopt opts 'batchsize (config 'batchsize 10000)))
	 (nthreads (mt/threadcount (getopt opts 'nthreads (config 'nthreads #t))))
	 (batches (batchup items batchsize nthreads))
	 (fifo (fifo/make batches `#[fillfn ,fifo/exhausted!]))
	 (before (getopt opts 'before #f))
	 (after (getopt opts 'after #f))
	 (progress (getopt opts 'progress #f))
	 (state (getopt opts 'state #f))
	 (loop-state #[])
	 (threads {})
	 (count 0))
    (when (and nthreads (> nthreads (length batches)))
      (set! nthreads (length batches)))
    (when state (store! state 'loop loop-state))
    (lognotice |FIFOEngine| 
      "Processing " ($count (choice-size items)) " items "
      "in " ($count (length batches)) " batches using " nthreads " threads "
      "with\n   " fcn)
    (dotimes (i nthreads)
      (set+! threads 
	(thread/call batch-step 
	    fcn fifo opts 
	    loop-state state 
	    before after progress)
	(sleep nthreads)))
    (thread/wait threads)
    (commit)))

(define (engine/fetchoids oids (state #f))
  (prefetch-oids! oids))
(define (engine/fetchoids oids (state #f))
  (prefetch-keys! oids))

(define (engine/poolfetch pool)
  (ambda (oids (state #f)) (pool-prefetch! pool oids)))

(define (engine/indexfetch index)
  (ambda (keys (state #f)) (prefetch-keys! index keys)))

(define (engine/swapout arg)
  (ambda (keys (state #f)) (swapout arg)))

(define (engine/savepool pool)
  (ambda (keys (state #f)) (commit pool)))

(define (engine/saveindex index)
  (ambda (keys (state #f)) (commit index)))

(define (engine/savetable table file)
  (ambda (keys (state #f)) (dtype->file table file)))

(module-export! '{engine/fetchoids engine/fetchkeys 
		  engine/poolfetch engine/indexfetch engine/swapout 
		  engine/savepool engine/saveindex engine/savetable})
