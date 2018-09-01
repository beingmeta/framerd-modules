#!/usr/bin/fdexec
;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(define english @1/2c1c7)

(defambda (index-words f (batch-state #f))
  (prefetch-oids! f)
  (let* ((loop-state (get batch-state 'loop))
	 (words.index (getopt loop-state 'words.index))
	 (frags.index (getopt loop-state 'frags.index))
	 (norms.index  (getopt loop-state 'norms.index))
	 (indicators.index  (getopt loop-state 'indicators.index))
	 (glosses.index  (getopt loop-state 'glosses.index))
	 (other.index  (getopt loop-state 'other.index)))
    (do-choices f
      (unless (test f 'source @1/1)
	(let* ((%words (get f '%words))
	       (%norms (get f '%norms))
	       (%glosses (get f '%glosses))
	       (%indicators (get f '%indicators))
	       (langids (getkeys {%words %norms %glosses %indicators})))
	  (do-choices (langid langids)
	    (index-string words.index f (get language-map langid)
			  (get %words langid))
	    (index-frags frags.index f (get frag-map langid)
			 (pick (get %words langid) compound-string?)
			 2 #t)
	    (index-string norms.index f (get norm-map langid)
			  (get %norms langid))
	    (index-string norms.index f (get indicator-map langid)
			  (get %indicators langid))
	    (index-gloss glosses.index f (get gloss-map langid)
			 (get %glosses langid)))))
      (swapout f))))

(define (main)
  (config! 'appid "indexwords")
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup}))
  (let* ((pools (use-pool (mkpath indir brico-pool-names)))
	 (words.index (target-index "words.index"))
	 (frags.index (target-index "frags.index"))
	 (indicators.index (target-index "indicators.index"))
	 (glosses.index (target-index "glosses.index"))
	 (norms.index (target-index "norms.index"))
	 (other.index (target-index "other.index"))
	 (oids (pool-elts pools)))
    (engine/run index-words oids
      `#[loop #[words.index ,words.index 
		frags.index ,frags.index
		indicators.index ,indicators.index
		glosses.index ,glosses.index
		norms.index ,norms.index
		other.index ,other.index]
	 counters {words names}
	 logcounters #(words names)
	 batchsize ,(config 'batchsize 5000)
	 logfreq ,(config 'logfreq 50)
	 checkfreq 15
	 checktests ,(engine/delta 'items 100000)
	 checkpoint ,{pools words.index frags.index 
		      indicators.index
		      norms.index glosses.index
		      other.index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t])))
