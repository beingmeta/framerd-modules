;;(load "brico.scm")

(use-module '{brico brico/lookup
	      brico/dterms mttools stringfmts engine fifo
	      logger varconfig
	      optimize})

(config! 'cachelevel 2)
(config! 'log:elapsed #t)
(config! 'log:threadid #t)

;;; The output file

(define datadir (get-component "output"))
(varconfig! output datadir)

(define toplangs-init
  '#("en" "nl" "fr" "es" "it" "de" "pl" "sv" "zh" "fi" "pt" "ja" "ko"))

(define (open-outfile filename)
  (when (and (config 'rebuild #f config:boolean)
	     (file-exists? filename))
    (logwarn |Rebuild| "Moving " filename " to " (glom filename ".bak"))
    (move-file filename (glom filename ".bak")))
  (if (has-suffix filename ".table")
      (if (file-exists? filename)
	  (file->dtype filename)
	  (make-hashtable 2000000))
      (if (file-exists? filename)
	  (open-index filename #[register #t])
	  (make-index filename #[type hashindex size 8000000 register #t]))))

;;; Getting location dterms

(define (get-location-dterm f language)
  (let ((norm (get-norm f language))
	(cnorm (get-norm (get f 'country) language))
	(rnorm (get-norm (get f 'region) language))
	(langnorm (get norm-map language)))
    (cond ((singleton? (?? (choice langnorm language) norm
			   @?partof* (?? (choice langnorm language)
					 cnorm)))
	   (string-append norm ", " cnorm))
	  ((singleton? (?? (choice langnorm language) norm
			   @?partof* (?? (choice langnorm language)
					 rnorm)))
	   (string-append norm ", " rnorm))
	  (else (fail)))))

(define (get-location-dterm/prefetch f language langnorm)
  (prefetch-oids! f)
  (let ((r+c (get f '{region country})))
    (prefetch-oids! r+c)
    (let ((norms (get-norm f language))
	  (r+c-norms (get-norm r+c language)))
      (find-frames-prefetch!
       (choice language langnorm) (choice norms r+c-norms))
      (find-frames-prefetch!
       @?partof* (?? (choice language langnorm) r+c-norms)))))


(defambda (dolocationdterms language index)
  (message "Computing location dterms in " language)
  (let ((langnorm (get norm-map language))
	(candidates (?? 'sensecat '{noun.location noun.object noun.artifact}
			'has '{country region}))
	(already-done (cdr (pick (getkeys index) language))))
    (do-choices-mt (f (difference candidates already-done)
		      (config 'nthreads 4)
		      (lambda (f done)
			(when done (commit) (clearcaches))
			(unless done
			  (get-location-dterm/prefetch
			   (qc f) language langnorm)))
		      (config 'blocksize 5000))
      (let ((dterm (get-location-dterm f language)))
	(when (exists? dterm) (add! index (cons language f) dterm))))))

;;; Getting simple dterms

(defambda (generate-english-dterms frames output)
  (let ((table (make-hashtable))
	(norm-index (pick (config 'indexes) index-source has-suffix "/en_norms.index"))
	(word-index (pick (config 'indexes) index-source has-suffix "/en.index")))
    (prefetch-oids! frames)
    (lognotice |Batch| (choice-size frames) " frames")
    (do-choices (f frames)
      (add! table (cons @?en f) (get-dterm f @?en)))
    (swapout frames)
    (index-merge! output table)))

(defambda (generate-generic-dterms frames language output)
  (let ((table (make-hashtable))
	(norm-index (pick (config 'indexes) index-source has-suffix "/norms.index"))
	(word-index (pick (config 'indexes) index-source has-suffix "/words.index")))
    (prefetch-oids! frames)
    (let ((words (for-choices (f frames)
		   {(get (get f '%words) (get language 'key))
		    (get (get f '%norms) language)})))
      (prefetch-keys! word-index words)
      (prefetch-keys! norm-index words))
    (do-choices (f frames)
      (add! table (cons language f) (get-dterm f language)))
    (swapout frames)
    (index-merge! output table)))

;;; Doing the cache with multiple threads

(defambda (cachedterms frames language index)
  (do-choices-mt (f frames (config 'corethreads 24)
		    (lambda (f done)
		      (when done (commit) (clearcaches))
		      (unless done
			(prefetch-oids! f)
			(find-frames-prefetch!
			 (choice language (get norm-map language))
			 (get-norm f language))))
		    (config 'blocksize 2000)
		    (or (config 'mtverbose) 4))
    (let ((dterm (get-dterm f language)))
      (when (exists? dterm) (message  "Found dterm " (write dterm) " for " f))
      (add! index (cons language f) (try dterm  #f)))))

(define (table-top table n)
  (if (< (table-size table) n)
      (getkeys table)
      (let ((keyvec (rsorted (getkeys table) table)))
	(elts keyvec 0 n))))

(optimize! '{brico brico/lookup brico/dterms})

;;; Getting most commonly referenced concepts and their dterms

 (define (make-common-dterms howmany language index (ft #f))
  (let ((freqtable (or ft (get-concept-refcounts))))
    (message "Dropping already mapped frames from freqtable")
    (config! 'appid (stringout "makedtermcache/GETCOMMON " language))
    (drop! freqtable (cdr (pick (getkeys index) language)))
    (let ((frames (if howmany
		      (table-top freqtable howmany)
		      (getkeys freqtable))))
      (notify "Caching dterms in " (get language '%id)
	      " for " (printnum (choice-size frames)) " concepts")
      (message "Caching dterms in " (get language '%id)
	       " for " (printnum (choice-size frames)) " concepts")
      (cachedterms frames language index))))

(define (get-concept-refcounts)
  (config! 'appid "makedtermcache/GETREFCOUNTS")
  (let ((freqtable (make-hashtable 8000000)))
    (notify "Getting concept reference counts")
    (notify "Assimilating WordNet frequency information")
    (do-choices-mt (f (?? 'type 'wordform)
		      (config 'nthreads 4) mt/fetchoids 25000)
      (hashtable-increment! freqtable (get f 'of) (try (get f 'freq) 1)))
    (count-oid-values  "dist/links.index" freqtable)
    (count-oid-values  "dist/refs.index" freqtable)
    (count-oid-values  "dist/entails.index" freqtable)
    (notify "Finished counting " (printnum (table-size freqtable))
	    " oid values")
    freqtable))

(define (count-oid-values index table)
  (let ((index (if (string? index) (open-index index) index)))
    (message "Counting values from " index)
    (notify "Counting values from " index)
    (do-choices-mt (key (index-keys index) 16
			(lambda (keys done)
			  (when done (clearcaches))
			  (unless done (prefetch-keys! index keys)))
			16384
			1)
      (when  (pair? key)
	(when (oid? (cdr key))
	  (hashtable-increment! table (cdr key)
	    (choice-size (get index key))))
	(when (and (pair? (cdr key)) (oid? (cadr key)))
	  (hashtable-increment! table (cadr key)
	    (* 3 (choice-size (get index key)))))))))

(define (get-implies-refcount index)
  (let ((table (make-hashtable))
	(index (if (string? index) (open-index index) index)))
    (message "Counting directly implied values from " index)
    (notify "Counting directly implied values from " index)
    (do-choices-mt (key (index-keys index) 16
			(lambda (keys done)
			  (when done (clearcaches))
			  (unless done (prefetch-keys! index keys)))
			16384
			1)
      (when  (and (pair? key) (pair? (cdr key))
		  (eq? (car key) @?implies))
	(hashtable-increment! table (cadr key)
	  (choice-size (get index key)))))
    table))

;;; The main event

(defambda (generate-dterms frames batch-state loop-state task-state)
  (let ((language (get loop-state 'language))
	(output (get loop-state 'output)))
    (if (eq? language @?en)
	(generate-english-dterms frames output)
	(generate-generic-dterms frames language output))))

(define (main (lang "en")
	      (outfile (mkpath datadir "dtermcache.index"))
	      (howmany #f))
  (config! 'appid (glom "dtermcache." lang))
  (let* ((index (open-outfile outfile))
	 (language (get language-map lang)))
    (engine/run generate-dterms	(pool-elts {brico.pool xbrico.pool names.pool})
      `#[loop #[language ,language output ,index]
	 batchsize 5000
	 checkfreq 15
	 checkpoint ,index
	 checktests ,(engine/delta 'items 100000)
	 logfreq 45
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t])))

(when (config 'optimize #t)
  (optimize! '{brico brico/lookup brico/dterms})
  (optimize! '{fifo engine})
  (optimize!))

#|
(begin (config! 'usedist #t) (config! 'cachelevel 3) (config! 'mtverbose 4)
       (load "scripts/makedtermcache.scm")
       (make-hash-index "tmp.index" 8000000)
       (define ix (open-index "tmp.index")))
|#
