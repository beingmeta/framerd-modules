#!/usr/bin/fdexec
;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(define english @1/2c1c7)

(define enorm (get norm-map english))
(define enindexes (get indicator-map english))

(define frags (get frag-map english))
(define cues (get indicator-map english))
(define norm (get norm-map english))

(defambda (index-english f (batch-state #f))
  (prefetch-oids! f)
  (let* ((loop-state (get batch-state 'loop))
	 (words.index (getopt loop-state 'words.index))
	 (frags.index (getopt loop-state 'frags.index))
	 (norms.index  (getopt loop-state 'norms.index))
	 (glosses.index  (getopt loop-state 'glosses.index))
	 (indicators.index  (getopt loop-state 'indicators.index))
	 (other.index  (getopt loop-state 'other.index))
	 (names.index (getopt loop-state 'names.index))
	 (word-count 0)
	 (name-count 0))
    (do-choices f
      (unless  (test f 'source @1/1)
	(let* ((words {(get f 'words) (get (get f '%words) 'en)})
	       (norms (get (get f '%norm) english))
	       (indicators (get (get f '%indicators) english))
	       (names {(pick words capitalized?) (get f 'names)
		       (pick (get f '{family lastname}) string?)}))
	  (index-string words.index f english words)
	  (index-string norms.index f norm norms)
	  (index-string indicators.index f cues indicators)
	  (index-frags frags.index f frags words 1 #t)
	  (index-string names.index f 'names (downcase names))
	  (set! word-count (+ word-count (choice-size words)))
	  (set! name-count (+ word-count (choice-size names)))
	  (index-string names.index f 'names names))
	(do-choices (gloss {(get f 'gloss) (get (get f '%glosses) @?en)})
	  (index-gloss glosses.index f @?engloss gloss))
	(index-string other.index f '{family lastname})))
    (swapout f)))

(define (main)
  (config! 'appid "indexenglish")
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup}))
  (let* ((pools (use-pool (mkpath indir brico-pool-names)))
	 (words.index (target-index "en.index" #[keyslot @?en]))
	 (frags.index (target-index "en_frags.index" #[keyslot @?en_frags]))
	 (indicators.index 
	  (target-index "en_indicators.index" `#[keyslot ,cues]))
	 (norms.index (target-index "en_norms.index" #[keyslot @?en_norms]))
	 (glosses.index (target-index "en_glosses.index" #[keyslot @?engloss]))
	 (names.index (target-index "en_names.index" #[keyslot names]))
	 (other.index (target-index "en_other.index"))
	 (oids (pool-elts pools)))
    (engine/run index-english oids
      `#[loop #[words.index ,words.index 
		frags.index ,frags.index
		indicators.index ,indicators.index
		norms.index ,norms.index
		glosses.index ,glosses.index
		other.index ,other.index
		names.index ,names.index]
	 counters {words names}
	 logcounters #(words names)
	 batchsize ,(config 'batchsize 5000)
	 logfreq ,(config 'logfreq 60)
	 checkfreq 15
	 checktests ,(engine/delta 'items 100000)
	 checkpoint ,{pools 
		      words.index frags.index
		      indicators.index norms.index
		      glosses.index
		      names.index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t
	 logfreq ,(config 'logfreq 50)])))
