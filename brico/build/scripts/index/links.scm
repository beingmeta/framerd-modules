#!/usr/bin/fdexec
;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(config! 'indexinfer #f)

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (indexer/prefetch (qc oids))))

(define misc-index (target-file "links.index"))
(define links-index (target-file "links.index"))
(define entails-index (target-file "links.index"))
(define entailed-index (target-file "links.index"))
(define refs-index (target-file "links.index"))
(define sumrefs-index (target-file "links.index"))
(define diffterms-index (target-file "links.index"))

(define misc-slotids '{PERTAINYM REGION COUNTRY FAMILY LASTNAME})

(define done #f)
(define (bgcommit)
  (until done (sleep 15) (commit)))

(define (index-node f slotindex)
  (index-relations slotindex f)
  (index-refterms slotindex f)
  (do-choices (slotid misc-slotids)
    (index-frame (try (get slotindex slotid) (get slotindex '%default))
	f slotid (pickoids (get f slotid)))))

(define (index-links frames batch-state loop-state task-state)
  (let* ((targets (get loop-state 'targets))
	 (thread-indexes (make-threadindex targets)))
    (prefetch-oids! frames)
    (indexer/prefetch frames)
    (do-choices (f frames) (index-node f thread-indexes))
    (threadindex/merge! targets thread-indexes)
    (swapout frames)))

(define (main)
  (let* ((pools (use-pool (mkpath indir brico-pool-names)))
	 (refs.index (target-index refs-index))
	 (sumrefs.index (target-index sumrefs-index))
	 (entails.index (target-index entails-index `#[keyslot ,entails]))
	 (entailed.index (target-index entailed-index `#[keyslot ,entailedby]))
	 (diffterms.index (target-index diffterms-index `#[keyslot ,diffterms]))
	 (links.index (target-index links-index))
	 (target
	  (frame-create #f
	    entails entails.index entailedby entailed.index
	    diffterms diffterms.index
	    {refterms references} refs.index
	    {sumterms /sumterms diffterms} sumrefs.index
	     misc-slotids (target-index misc-index)
	    '%default links.index)))
    (engine/run index-links (pool-elts pools)
      `#[loop #[targets ,target]
	 batchsize 2000 batchrange 3
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools (pick (getvalues target) index?)}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t
	 logfreq 25])) )

(when (config 'optimize #t)
  (optimize! '{brico engine fifo brico/indexing})
  (optimize!))
