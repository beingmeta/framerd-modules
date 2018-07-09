#!/usr/bin/fdexec
;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (indexer/prefetch (qc oids))))

(define (indexloop pool target)
  (do-choices-mt (f (pool-oids pool)
		    (config 'nthreads 4)
		    prefetcher (config 'blocksize 100000))
    (if (test f 'type 'wordform) (index-wordform target f)
	(if (test f 'words) (index-frame target f 'words)))))

(define (main pool . more)
  (config! 'appid (stringout "indexwordforms " pool))
  (optimize! 'brico 'brico/indexing indexloop)
  (let* ((pool (use-pool pool))
	 (target (target-index "wordforms.index")))
    (index-preamble pool " word form information")
    (indexloop pool target))
  (unless (null? more)
    (notify "Chaining indexwordforms to" (doseq (arg more) (printout " " arg)))
    (apply chain more)))

