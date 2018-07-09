#!/usr/bin/fdexec
;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (index-lattice/prefetch (qc oids))))

(define brico-pools (choice brico-pool xbrico-pool names-pool places-pool))
(define normal-slots (?? 'type 'slot))

(define relterm-slotids
  (choice relterms parts members ingredients partof memberof))

(defambda (index-slot index frame slot (values))
  (if (bound? values)
      (index-frame (try (get index slot) (get index '%default))
	  frame slot values)
      (index-frame (try (get index slot) (get index '%default))
	  frame slot)))

(defambda (index-phase1 concepts batch-state loop-state task-state)
  (let ((indexes (get loop-state 'indexes)))
    (prefetch-oids! concepts)
    (do-choices (concept (%pick concepts '{words %words names hypernym @?genls}))
      ;; ALWAYS is transitive
      (index-frame* indexes concept always always /always)
      (index-slot indexes concept always (list (get concept always)))
      ;; indexes the inverse relationship
      (index-slot indexes concept /always (%get concept /always))
      (index-slot indexes concept /always (list (get concept /always)))
      ;; This is for cross-domain relationships
      (index-slot indexes (%get concept /always) always concept)
      ;; ALWAYS implies sometimes
      ;; but can also compute it at search time
      ;; (index-frame* indexes concept always sometimes sometimes)
      ;; SOMETIMES is symmetric
      (index-slot indexes concept sometimes)
      (index-slot indexes (get concept sometimes) sometimes concept)
      (index-slot indexes concept (list (get concept sometimes)))
      ;; NEVER is symmetric
      (index-slot indexes concept never)
      (index-slot indexes (get concept never) never concept)
      (index-slot indexes concept never (list (get concept never)))
      ;; SOMENOT is not symmetric
      (index-slot indexes concept somenot)
      (index-slot indexes (get concept somenot) /somenot concept)
      (index-slot indexes concept somenot (list (get concept somenot)))
      ;; indexes the inverse relationship
      (index-slot indexes concept /somenot (%get concept /somenot))
      (index-slot indexes concept /somenot (list (%get concept /somenot)))
      (index-slot indexes (%get concept /somenot) somenot concept)
      ;; indexes probablistic slots
      (index-slot indexes concept commonly)
      (index-slot indexes concept /commonly (%get concept /commonly))
      (index-slot indexes concept rarely)
      (index-slot indexes concept /rarely (%get concept /rarely))
      ;; These are for cross-domain relationships
      (index-slot indexes (%get concept /commonly) commonly concept)
      (index-slot indexes (%get concept /rarely) rarely concept)
      ;; indexes features
      (index-slot indexes concept relterms (%get concept relterm-slotids))
      (let ((feature-slotids
	     (difference (choice (pick (getkeys concept) brico-pools))
			 normal-slots)))
	(index-slot indexes concept relterms (%get concept feature-slotids))
	(do-choices (fs feature-slotids)
	  (index-slot indexes concept fs (list (get concept fs))))
	(do-choices (fs feature-slotids)
	  (index-slot indexes concept fs (get concept fs)))))
    (swapout concepts)))

(defambda (index-phase2 concepts batch-state loop-state task-state)
  (let ((indexes (get loop-state 'indexes)))
    (prefetch-oids! concepts)
    (do-choices (concept (%pick concepts '{words %words names hypernym @?genls}))
      (let ((s (get concept sometimes))
	    (n (get concept never)))
	(when (exists? s)
	  (index-slot indexes concept sometimes (list s))
	  (index-slot indexes s sometimes (list concept))
	  (index-slot indexes concept
		      sometimes (find-frames indexes /always s))
	  (unless (test concept 'type 'individual)
	    (index-slot indexes (find-frames indexes /always s)
			sometimes concept)))
	(when (exists? n)
	  (index-slot indexes concept never (list n))
	  (index-slot indexes n never (list concept))
	  (index-slot indexes (find-frames indexes /always n)
		      never concept))
	(index-slot indexes concept probably
		    (find-frames indexes /always (get concept probably)))))
    (swapout concepts)))

(define (main . names)
  (let* ((pools (use-pool (if (empty-list? names)
			      (mkpath indir brico-pool-names)
			      (elts names))))
	 (always.index (target-index "always.index" `#[keyslot @?always]))
	 (always_inv.index (target-index "always_inv.index" `#[keyslot @?/always]))
	 (never.index (target-index "never.index"  `#[keyslot @?never]))
	 (sometimes.index (target-index "sometimes.index" `#[keyslot @?sometimes]))
	 (termlogic.index (target-index "termlogic.index"))
	 (target `#[@?always ,always.index
		    @?/always ,always_inv.index
		    @?sometimes ,sometimes.index
		    @?never ,never.index
		    @?sometimes ,sometimes.index
		    %default ,termlogic.index]))
    (engine/run (if (config 'phase2 #f) index-phase2 index-phase1)
	(pool-elts pools)
      `#[loop #[indexes ,target]
	 batchsize 25000 batchrange 4
	 nthreads ,(config 'nthreads #t)
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools (get target (getkeys target))}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t
	 logfreq 25])))

(optimize! '{brico engine fifo brico/indexing})
(optimize!)
