#!/usr/bin/fdexec
;;; -*- Mode: Scheme; -*-

(load-component "indexbase.scm")

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (index-lattice/prefetch (qc oids))))

(define (index-links* frames batch-state loop-state task-state)
  (let* ((index (get loop-state 'index))
	 (thread-index (make-hashtable)))
    (prefetch-oids! frames)
    ;; (index-lattice/prefetch (qc frames))
    (do-choices (f frames) (index-lattice f thread-index))
    (index/merge! index thread-index)
    (swapout frames)))

(define (main)
  (let* ((pools (use-pool (mkpath indir {"brico.pool" "xbrico.pool" "names.pool" "places.pool"})))
	 (always.index (target-index "always.index"))
	 (allways.index (target-index "allways.index"))
	 (always_inv.index (target-index "always_inv.index"))
	 (allways_inv.index (target-index "allways_inv.index"))
	 (genls.index (target-index "genls.index"))
	 (specls.index (target-index "specls.index"))
	 (allgenls.index (target-index "allgenls.index"))
	 (allspecls.index (target-index "allspecls.index"))
	 (lattice.index (target-index "lattice.index"))
	 (target #[@?genls genls.index @?specls specls.index
		   @?always always.index @?always]))
    (engine/run index-links* (pool-elts pools)
      `#[loop #[index ,lattice.index]
	 batchsize 25000 batchrange 4
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools lattice.index}
	 logfns {,engine/log ,engine/logrusage}
	 logfreq ,(config 'logfreq 50)
	 logchecks #t])))

(optimize! '{brico engine fifo brico/indexing})
(optimize!)
