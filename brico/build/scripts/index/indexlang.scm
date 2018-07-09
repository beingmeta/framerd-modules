#!/usr/bin/fdexec
;;; -*- Mode: Scheme; -*-

(load-component "indexbase.scm")

(define words-index (target-file "english.index"))
(define extras-index (target-file "enplus.index"))

(define english @1/2c1c7)

(define enorm (get norm-map english))
(define enindexes (get index-map english))

(define enfrags (get frag-map english))

(defambda (index-english f (loop-state #f))
  (prefetch-oids! f)
  (let* ((word-index (getopt loop-state 'words))
	 (frag-index (getopt loop-state 'frags))
	 (norm-index  (getopt loop-state 'norm))
	 (cue-index  (getopt loop-state 'cue))
	 (misc-index  (getopt loop-state 'cue))
	 (words english)
	 (frags (get frag-map words))
	 (norm (get norm-map english))
	 (cues (get indexmap-map english))
	(name-index (getopt loop-state 'names)))
    (do-choices f
      (unless (or (test f 'source @1/1) (not (test f 'source)))
	(let ((words {(get f 'words) (get (get f '%words) 'en)}))
	  (index-string word-index f english words)
	  (index-frags frag-index f frags (get f 'words) 1 #t))
	(when (test f '%norm)
	  (index-string norm-index f norm (get (get f '%norm) english)))
	(when  (test f '%indexes)
	  (index-string cue-index f cues (get (get f '%indexes) english)))
	(index-string name-index f 'names 
		      {(get f 'names)
		       (pick (pick (get f '{family lastname}) string?)
			 somecap?)}
		      #f)))
    (swapout f)))

(define (main)
  (config! 'appid (stringout "indexenglish " pool))
  (when (config 'optimize #t) (optimize! '{engine brico brico/indexing brico/lookup}))
  (let* ((pools (use-pool (mkpath indir {"brico.pool" "xbrico.pool" "names.pool" "places.pool"})))
	 (word-index (target-index (mkpath outdir "en.index")))
	 (frags-index (target-index (mkpath outdir "en_frags.index")))
	 (cues-index (target-index (mkpath outdir "en_cues.index")))
	 (norm-index (target-index (mkpath outdir "en_norm.index")))
	 (oids (pool-elts pools)))
    (engine/run index-english oids
      `#[loop #[words ,words.index 
		frags ,frags.index
		cues ,cues.index
		norm ,norms.index]
	 counters {words names}
	 logcounters #(words names)
	 batchsize ,(config 'batchsize 5000)
	 logfreq ,(config 'logfreq 60)
	 checkfreq 15
	 checktests ,(engine/delta 'items 100000)
	 checkpoint ,{pools word-index frags-index cues-index norm-index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t])))



