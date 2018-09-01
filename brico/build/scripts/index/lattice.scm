#!/usr/bin/fdexec
;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (index-lattice/prefetch (qc oids))))

(define (index-node f thread-index)
  (index-frame* thread-index f genls* genls specls*)
  (index-frame* thread-index f partof* partof parts*)
  (index-frame* thread-index f memberof* memberof members*)
  (index-frame* thread-index f ingredientof* ingredientof ingredients*))

(defambda (index-batch frames batch-state loop-state task-state)
  (let* ((index (get loop-state 'index))
	 (thread-index (if (config 'THREADINDEX #t config:boolean)
			   (make-threadindex index)
			   index)))
    (prefetch-oids! frames)
    (do-choices (f frames) (index-node f thread-index))
    (unless (eq? index thread-index)
      (threadindex/merge! index thread-index))
    (swapout frames)))

(define indexes #f)

(define (get-index)
  (or indexes
      (let ((index
	     (frame-create #f
	       @?genls (target-index "lattice.index")
	       @?specls (target-index "lattice.index")
	       @?genls* (target-index "lattice.index")
	       @?specls* (target-index "lattice.index")
	       {@?parts @?partof @?partof* @?parts*} (target-index "divisions.index")
	       {@?members @?memberof @?members* @?memberof*} (target-index "divisions.index")
	       {@?ingredients @?ingredientof @?ingredients* @?ingredientof*}
	       (target-index "divisions.index")
	       '%default (target-index "lattice.index"))))
	(set! indexes index)
	index)))

(define (main)
  (let* ((pools (use-pool (mkpath indir brico-pool-names)))
	 (frames (if (config 'JUST)
		     (sample-n (pool-elts pools) (config 'just))
		     (pool-elts pools)))
	 (target (get-index)))
    (engine/run index-batch frames
      `#[loop #[index ,target]
	 batchsize 25000 batchrange 4
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools (get target (getkeys target))}
	 logfns {,engine/log ,engine/logrusage}
	 logfreq ,(config 'logfreq 50)
	 logchecks #t])))

(when (config 'optimize #f config:boolean)
  (optimize! '{brico engine fifo brico/indexing})
  (optimize!))
