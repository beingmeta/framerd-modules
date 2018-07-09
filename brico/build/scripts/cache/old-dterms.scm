(load "brico.scm")

(use-module '{brico brico/lookup brico/dterms mttools stringfmts})

;;; The output file

(define datadir (get-component "data"))

(define toplangs-init
  '#("en" "nl" "fr" "es" "it" "de" "pl" "sv" "zh" "fi" "pt" "ja" "ko"))

(define (open-outfile filename)
  (if (has-suffix filename ".table")
      (if (file-exists? filename)
	  (file->dtype filename)
	  (make-hashtable 2000000))
      (if (file-exists? filename)
	  (open-index filename)
	  (let* ((toplangs (map (lambda (x) (get language-map x))
				toplangs-init))
		 (otherlangs (difference (?? 'type 'language)
					 (elts toplangs)))
		 (init-slotids (append toplangs
				       (sorted otherlangs))))
	    (make-hash-index filename 8000000 init-slotids)
	    (open-index filename)))))

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

(defambda (dosimpledterms pool language index)
  (config! 'appid (stringout "makedtermcache/SIMPLEDTERMS " (pool-label pool)))
  (notify "Getting simple dterms in " language " from " pool)
  (let ((langnorm (get norm-map language))
	(already-done (cdr (pick (getkeys index) language)))
	(deftermed (choice->hashset (?? 'has @?sumterms))))
    (do-choices-mt (f (difference (pool-elts pool) already-done)
		      (config 'nthreads 4)
		      (lambda (f done)
			(when done (commit) (clearcaches))
			(unless done
			  (prefetch-oids! f)
			  (find-frames-prefetch!
			   language (for-choices f (get (get f '%norm) language)))))
		      (config 'blocksize 10000)
		      (or (config 'mtverbose) 4))
      (let ((norm (get (get f '%norm) language)))
	(when (and (exists? norm)
		   (identical? f (pick (?? language norm) deftermed)))
	  (if (and (not (in-pool? f brico-pool))
		   (test f 'sensecat
			 '{noun.location noun.artifact noun.object})
		   (%test f @?implies)
		   (test f 'country))
	      (add! index (cons language f)
		    (string-append norm ", " (get-norm (get f 'country))))
	      (add! index (cons language f) norm)))))))

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

;;; Make isa/genls dterms

(define (find-predicate-frames)
  (config! 'appid "makedtermcache/FINDPREDICATES")
  (message "Finding predicate terms in the knowledge base")
  (notify "Finding predicate terms in the knowledge base")
  (let ((frameset (make-hashset)))
    (do-choices-mt (f (?? 'has '{@?isa @?genls hypernym}) 4 mt/fetchoids 200000)
      (hashset-add! frameset (%get f '{@?isa @?genls hypernym})))
    (hashset-elts frameset)))

(defambda (find-unprocessed frames language cache)
  (message "Determining unprocessed predicates across " (choice-size frames) " candidates")
  (notify "Determining unprocessed predicates across " (choice-size frames) " candidates")
  (prefetch-keys! cache (cons language frames))
  (message "Done with cache prefetch")
  (filter-choices (f frames)
    (not (test cache (cons language f)))))

(defambda (make-predicates-loop cache todo language langnorm)
  (do-choices-mt (f todo 8
		    (lambda (f done)
		      (when done (commit) (clearcaches))
		      (unless done
			(prefetch-oids! f)
			(find-frames-prefetch!
			 (choice language langnorm)
			 (get-norm f language))))
		    1024)
    (let ((dterm (get-dterm f language))) ;; (ipeval (get-dterm f language))
      (when (exists? dterm) (message "Found predicate dterm " dterm " for " f))
      (store! cache (cons language f) (try dterm #f)))))

(define (make-dterm-predicates language cache)
  (let* ((predicates (find-predicate-frames))
	 (todo (find-unprocessed predicates language cache))
	 (langnorm (get norm-map language)))
    (config! 'appid (stringout "makedtermcache/PREDICATES " language))
    (message "Generating dterms for "
	     (choice-size predicates) " predicates in " language)
    (notify "Generating dterms for "
	    (choice-size predicates) " predicates in " language)
    (make-predicates-loop cache todo language langnorm)))

(define (checkpoint state)
  (commit)
  (dtype->file state "makedtermcache.state"))

;;; The main event

(define (main (lang "en") (howmany #f)
	      (outfile (mkpath datadir "dtermcache.index")))
  (config! 'appid "MAKEDTERMCACHE")
  (let* ((index (open-outfile outfile))
	 (language (get language-map lang)))
    (dosimpledterms brico-pool language index)
    (checkpoint "simplebrico")
    (dosimpledterms xbrico-pool language index)
    (checkpoint "simplexbrico")
    (dosimpledterms names-pool language index)
    (checkpoint "simplenames")
    ;; (dolocationdterms language index)
    (make-dterm-predicates language index)
    (checkpoint "predicatedterms")
    (make-common-dterms howmany language index)
    (checkpoint "commondterms")
    (unless (index? index) (dtype->file index outfile 524288))))

(optimize! '{brico brico/lookup brico/dterms})
(optimize!)

#|
(begin (config! 'usedist #t) (config! 'cachelevel 3) (config! 'mtverbose 4)
       (load "scripts/makedtermcache.scm")
       (make-hash-index "tmp.index" 8000000)
       (define ix (open-index "tmp.index")))
|#
