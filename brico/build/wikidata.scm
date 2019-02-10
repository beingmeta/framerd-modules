;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/wikidata)

(use-module '{texttools fdweb logger varconfig stringfmts optimize})
(use-module '{storage/flex storage/typeindex storage/aggregates})
(use-module '{brico brico/indexing})

(module-export! '{wikidata-dir wikid.pool wikid.map 
		  wikidata.index wikids.index meta.index
		  wikid wikid/ingest
		  wikid/read wikid/readn wikid/fetch
		  wikid/commit})

(define %loglevel %info%)

(define wikidata-dir #f)
(define wikidata-default-dir (get-component "wikidata/"))

(define *epoch* "testing")
(varconfig! wikidata:epoch *epoch*)

(define block-size 25000)
(define merge-size 5000)

(varconfig! wikidata:blocksize block-size)
(varconfig! wikidata:mergesize merge-size)

(comment
  (config! 'dbloglevel %info%)
  (config! 'cachelevel 2)
  (define brico-dir (get-component "new/"))
  (config! 'bricosource brico-dir)
  (define wikidata-input-file
    (get-component "wikisrc/latest-all.json.bz2"))
  (indexctl meta.index 'readonly #f)
  (poolctl brico.pool 'readonly #f))

(define wikid.pool #f)
(define wikid.map #f)

(define wikids.index #f)
(define misc.index #f)
(define en_labels.index #f)
(define en_aliases.index #f)
(define aliases.index #f)
(define links.index #f)
(define graph.index #f)
(define words.index #f)
(define norms.index #f)
(define en.index #f)
(define en_norms.index #f)
(define wikid_types.index #f)
(define wikid_has.index #f)


(define-init base-wikid-load #f)
(define-init base-brico-load #f)

(define meta.index #f)

(define wikidata.index #f)
(define wikidata.indexes #[])

(define (set-wikidata-root! dir)
  (unless (equal? wikidata-dir dir)
    (set! wikid.pool
      (db/ref (mkpath dir "wikid.flexpool")
	      `#[type flexpool 
		 base @31C1/0 capacity #gib partsize ,(* 4 #mib) 
		 create #t prefix "wikid/wikid" compression zstd
		 metadata 
		 #[adjuncts #[aliases
			      #[pool "aliases.flexpool" prefix "aliases" 
				compression zstd slotids #() create #t]
			      %glosses
			      #[pool "glosses.flexpool" prefix "glosses"
				compression zstd slotids #() create #t]
			      labels
			      #[pool "labels.flexpool" prefix "labels" 
				compression zstd slotids #() create #t]
			      links #[pool "links.flexpool" prefix "links" 
				      compression zstd slotids #() create #t]
			      %words
			      #[pool "words.flexpool" prefix "words" 
				compression zstd slotids #() create #t]
			      %norms
			      #[pool "norms.flexpool" prefix "norms" 
				compression zstd slotids #() create #t]]]
		 adjopts #[compression zstd]]))
    (set! base-wikid-load (pool-load wikid.pool))
    (set! base-brico-load (and brico.pool (pool-load brico.pool)))
    (set! wikid.map
      (db/ref (mkpath dir "wikid.map")
	      `#[indextype memindex
		 preload ,(config 'wikibuild #f)
		 keyslot wikid
		 register #t
		 create #t]))

    (set! wikids.index
      (db/ref (mkpath dir "wikids.index")
	      `#[type hashindex
		 size ,(* 128 #mib)
		 keyslot wikid
		 register #t
		 create #t]))
    (add! wikidata.indexes 'wikids wikids.index)

    (set! misc.index
      (db/ref (mkpath dir "misc.index")
	      `#[type hashindex
		 size ,(* 64 #mib)
		 register #t
		 create #t]))
    (add! wikidata.indexes 'misc misc.index)

    (set! graph.index
      (db/ref (mkpath dir "graph.index")
	      `#[type hashindex
		 size ,(* 128 #mib)
		 register #t
		 create #t]))
    (add! wikidata.indexes 'graph graph.index)

    (set! en_labels.index
      (db/ref (mkpath dir "en_labels.index")
	      `#[type hashindex size ,(* 64 #mib) register #t create #t
		 keyslot labels]))
    (add! wikidata.indexes 'labels en_labels.index)

    (set! en_aliases.index
      (db/ref (mkpath dir "en_aliases.index")
	      `#[type hashindex size ,(* 64 #mib) register #t create #t
		 keyslot aliases]))
    (add! wikidata.indexes 'aliases en_aliases.index)

    (set! links.index
      (db/ref (mkpath dir "links.index")
	      `#[type hashindex size ,(* 16 #mib) register #t create #t
		 keyslot links]))
    (store! wikidata.indexes 'links links.index)

    (set! words.index
      (db/ref (mkpath dir "words.index")
	      `#[type hashindex size ,(* 16 #mib) register #t create #t
		 keyslot words]))
    (store! wikidata.indexes 'links words.index)

    (set! norms.index
      (db/ref (mkpath dir "norms.index")
	      `#[type hashindex size ,(* 16 #mib) register #t create #t
		 keyslot norms]))
    (store! wikidata.indexes 'norms norms.index)

    (set! en.index
      (db/ref (mkpath dir "en.index")
	      #.[type 'hashindex size (* 16 #mib) register #t create #t
		      keyslot en]))
    (store! wikidata.indexes en en.index)

    (set! en_norms.index
      (db/ref (mkpath dir "en_norms.index")
	      #.[type 'hashindex size (* 16 #mib) register #t create #t
		      keyslot en_norms]))
    (store! wikidata.indexes en_norms en_norms.index)

    (unless (file-directory? (mkpath dir "types")) (mkdir (mkpath dir "types")))
    (set! wikid_types.index
      (typeindex/open (mkpath dir "types")
		      `#[type typeindex keyslot type 
			 register #t create #t]))
    (store! wikidata.indexes 'type wikid_types.index)

    (unless (file-directory? (mkpath dir "has")) (mkdir (mkpath dir "has")))
    (set! wikid_has.index
      (typeindex/open (mkpath dir "has")
		      `#[type typeindex keyslot has
			 register #t create #t]))
    (store! wikidata.indexes 'has wikid_has.index)

    (set! meta.index 
      (db/ref (mkpath (config 'bricosource) "core.index")
	      #[type hashindex register #t create #t]))

    (set! wikidata.index
      (make-aggregate-index {wikids.index misc.index graph.index
			     words.index norms.index
			     en.index en_norms.index
			     links.index
			     wikid_types.index
			     wikid_has.index}
			    #[register #t]))

    (set! wikidata-dir dir)

    ))

(config-def! 'wikidata:root
	     (lambda (var (val))
	       (cond ((not (bound? val)) wikidata-dir)
		     ((string? val) (set-wikidata-root! val))
		     (else (set-wikidata-root! wikidata-default-dir)))))

;;; Interning wikids

(define (wikid id (uuid #f))
  (try (get wikid.map id)
       (intern-wikid id uuid)))

(define intern-wikid
  (slambda (string uuid)
    (try (get wikid.map string)
	 (let* ((isprop (has-prefix string "P"))
		(existing 
		 (find-frames (if isprop meta.index wikids.index)
		   'wikid string))
		(frame
		 (try existing
		      (frame-create (if isprop brico.pool wikid.pool)
			'wikid string '%ID string 
			'uuid (tryif uuid uuid)
			'type (if isprop 'property 'entity)
			'source 'wikidata))))
	   (store! wikid.map string frame)
	   (index-frame wikids.index frame 'wikid)
	   (index-frame wikid_types.index frame 'type)
	   (when isprop (index-frame meta.index frame '{wikid type}))
	   frame))))

(define (getmodel string)
  (if (has-prefix string "http://www.wikidata.org/entity/")
      (wikid (strip-prefix string "http://www.wikidata.org/entity/"))
      string))

;;; Converting dump data

(define (expand-langid langid (dc))
  (set! dc (downcase langid))
  {(tryif (position #\_ dc) (string->symbol (slice dc 0 (position #\_ dc))))
   langid})

(defambda (->langmap map (xform #f))
  (tryif (exists? map)
    (let ((into #[]))
      (do-choices (assoc (getassocs map))
	(if (vector? (cdr assoc))
	    (add! into (expand-langid (car assoc)) 
		  (if xform
		      (xform (get (elts (cdr assoc)) 'value))
		      (get (elts (cdr assoc)) 'value)))
	    (add! into (expand-langid (car assoc)) 
		  (if xform
		      (xform (get (cdr assoc) 'value))
		      (get (cdr assoc) 'value)))))
      into)))
(define (->sitelinks map)
  (tryif (exists? map)
    (let ((into `#[]))
      (do-choices (assoc (getassocs map))
	(if (vector? (cdr assoc))
	    (store! into (expand-langid (car assoc))
		    (get (elts (cdr assoc)) 'title))
	    (store! into (expand-langid (car assoc))
		    (get (cdr assoc) 'title))))
      into)))

(define (expandvecs x) (if (vector? x) (elts x) x))

(define (mapping-keys mapping)
  (for-choices (slotid (getkeys mapping))
    (cons slotid (get mapping slotid))))

(define (copy-frame data into (index #f))
  (when (test data 'title) (store! into 'title (get data 'title)))
  (store! into 'type (string->symbol (upcase (get data 'type))))
  (let* ((xfn (if (capitalized? (get (get (get data 'labels) 'en) 'value))
		  #f downcase))
	 (norms (->langmap (get data 'labels) xfn))
	 (words (->langmap (get data 'aliases) xfn)))
    (when (exists? norms) (store! into '%norms norms))
    (when (exists? words) (store! into '%words words))
    (when (test norms 'en)
      (store! into 'norms (get norms 'en))
      (store! into 'words {(get words 'en) (get norms 'en)}))
    (store! into '%id
	    (if (exists? (get into 'norms))
		(list 'WIKID (get into 'wikid) (pick-one (get into 'norms)))
		(list 'WIKID (get into 'wikid)))))
  (when (test data 'descriptions)
    (store! into 'gloss (get (get (get data 'descriptions) 'en) 'value))
    (when (test data 'descriptions)
      (store! into 'glosses (->langmap (get data 'descriptions)))))
  (when (test data 'labels) (store! into 'labels (->langmap (get data 'labels))))
  (when (test data 'aliases) (store! into 'aliases (->langmap (get data 'aliases))))
  (when (test data 'sitelinks) (store! into 'links (->sitelinks (get data 'sitelinks))))
  (store! into 'modified (timestamp (get data 'modified)))
  (store! into 'revid (get data 'revid))
  (when (test data 'claims)
    (do-choices (claim (expandvecs (getvalues (get data 'claims))))
      (unless (test claim 'rank "deprecated")
	(let* ((main (get claim 'mainsnak))
	       (prop (wikid (get main 'property)))
	       (context `#[])
	       (value (snak-value main context)))
	  (add! into prop value)
	  (when (test claim 'rank "preferred") (add! context 'rank "preferred"))
	  (when (test claim 'qualifiers)
	    (do-choices (snak (expandvecs (getvalues (get claim 'qualifiers))))
	      (add! context (wikid (get snak 'property))
		    (snak-value snak))))
	  (when (oid? value)
	    (index-frame graph.index into prop value)
	    (unless (test claim 'qualifiers)
	      (index-frame graph.index into prop (list value))))
	  (when (exists? (getkeys context))
	    (add! into prop (cons value context)))))))
  (store! into 'epoch *epoch*)
  (when index 
    (index-frame index into 'type)
    (index-frame index into 'links (mapping-keys (get into 'links)))
    (index-frame index into 'has (getkeys into))
    (index-frame index into 'words (get into 'words))
    (index-frame index into 'norms (get into 'norms))
    ;; (index-frame index into 'aliases (mapping-keys (get into 'aliases)))
    )
  (when en_labels.index
    (index-frame en_labels.index into 'labels
		 (mapping-keys `#[en ,(get (get into 'aliases) 'en)])))
  (when index
    (index-string index into en (get into 'words) 2)
    (index-string index into en_norms (get into 'norms) 2))

  into)

;;; Handling snaks (bigger than bytes)

(define (snak-value snak (context #[]) (value) (type))
  (default! value (get snak 'datavalue))
  (default! type (get snak 'datatype))
  (cond ((equal? type "string") (get value 'value))
	((equal? type "monolingualtext")
	 (glom (get value 'language) "$" (get value 'text)))
	((overlaps? type {"wikibase-item" "wikibase-entityid"})
	 (try (wikid (get (get value 'value) 'id)) 
	      (tryif (and (test value 'numeric-id) (test value 'entity-type "item"))
		(wikid (glom "Q" (get value 'numeric-id))))
	      value))
	((or (equal? type "time") (test value 'time) (test value 'type "time"))
	 (if (has-prefix (get value 'time) "+")
	     (let* ((precision (get value 'precision))
		    (stamp (timestamp (slice (get value 'time) 1))))
	       (cond ((= precision 9) (store! stamp 'precision 'year))
		     ((= precision 10) (store! stamp 'precision 'month))
		     ((= precision 11) (store! stamp 'precision 'day))
		     ((= precision 12) (store! stamp 'precision 'hour))
		     ((= precision 13) (store! stamp 'precision 'minute))
		     ((= precision 14) (store! stamp 'precision 'second)))
	       (cond ((or (< (get stamp 'year) 1971) (> (get stamp 'year) 3000))
		      (when (test value 'calendarmodel)
			(store! context 'calendar-model (getmodel (get value 'calendarmodel))))
		      (get value 'time))
		     (else stamp)))
	     (if (has-prefix (get value 'time) "-")
		 (glom (slice (get value 'time) 1) "BCE")
		 value)))
	((equal? type "quantity")
	 (if (and (test value 'unit)
		  (not (test value 'unit 1))
		  (not (test value 'unit "1")))
	     (store! context 'unit (get value 'unit)))
	 (get value 'amount))
	((equal? type "globe-coordinate")
	 (when (test value 'globe)
	   (store! context 'globe (getmodel (get value 'globe))))
	 (when (test value 'precision)
	   (store! context 'precision (get value 'precision)))
	 (cons (get value 'latitude) (get value 'longitude)))
	(else (try (cons (get value 'value) (string->symbol type) )
		   (cons value (string->symbol type))))))

;;;; Getting data

(define (wikid/ingest data (index #f))
  (let ((f (wikid (get data 'id))))
    (unless (test f 'epoch *epoch*)
      (copy-frame data f index))
    f))

(define (wikid/read in)
  (prog1 (jsonparse in)
    (getline in ",")))

(define (readloop reader index (opts #f) (count 0) 
		  (limit) (merge)
		  (temp)
		  (results {})
		  (data))
  (default! limit (getopt opts 'limit block-size))
  (default! merge (getopt opts 'merge merge-size))
  (default! temp (aggregate/branch index))
  (set! data (onerror (jsonparse (reader)) #f))
  (while (and data (< count limit))
    (unless (empty-string? data)
      (set+! results (wikid/ingest data temp))
      (set! count (1+ count))
      (when (zero? (remainder count merge))
	(aggregate/merge! temp)
	(set! temp (aggregate/branch index))))
    (set! data (onerror (jsonparse (reader)) #f)))
  (aggregate/merge! temp)
  results)

(define (wikid/readn reader index (opts #f))
  (let ((nthreads (getopt opts 'nthreads (config 'nthreads (rusage 'ncpus))))
	(threads {}))
    (if (and nthreads (> nthreads 1))
	(begin (dotimes (i nthreads)
		 (set+! threads (thread/call readloop reader index opts)))
	  (thread/wait! threads)
	  (let ((errs (pick threads thread/error?)))
	    (if (exists? errs)
		(error |ThreadErrors| wikid/readn (thread/result errs)))))
	(readloop reader index opts))))

(define (wikid/fetch id (index #f))
  (let* ((fetched (urlget (glom "https://www.wikidata.org/wiki/Special:EntityData/" id ".json")))
	 (parsed (jsonparse (get fetched '%content))))
    (wikid/ingest parsed index)))

(define (wikid/commit)
  (flex/commit! 
   {wikid.pool wikidata.index meta.index wikid.map brico.pool
    (getvalues (poolctl wikid.pool 'adjuncts))})
  (commit))

;;;; Cycles

(define-init session-started #f)

(define (wikid/cycle r (opts #f))
  (let ((started (elapsed-time))
	(start-brico-load (pool-load brico.pool))
	(start-wikid-load (pool-load wikid.pool)))
    (unless session-started (set! session-started started))
    (wikid/readn r wikidata.index opts)
    (wikid/commit)
    (swapout)
    (lognotice |FinishedScan| 
      "Finished one cycle in " (secs->string (elapsed-time started)) " created "
      ($count (- (pool-load wikid.pool) start-wikid-load) "new item")
      " and "
      ($count (- (pool-load brico.pool) start-brico-load) "new property" "new properties"))
    (lognotice |Overall| 
      "Created " ($count (- (pool-load wikid.pool) base-wikid-load) "new item")
      " and "
      ($count (- (pool-load brico.pool) base-brico-load) "new property" "new properties")
      " in " (secs->string (elapsed-time session-started)))))


;;;; Listing properties from linked data

(define readprops-url
  "https://www.wikidata.org/w/api.php?action=wbsearchentities&search=P&language=en&type=property&format=json&limit=50")

(define property-ref
  (slambda (string)
    (get wikid.map string)
    (let ((f (frame-create brico.pool 'id string 'type '{property wikidata.property} 'source 'wikidata)))
      (store! wikid.map string f)
      f)))

(define (read-wikidata-properties (start 0))
  (let* ((fetched (urlget (glom readprops-url "&continue=" start)))
	 (parsed (jsonparse (get fetched '%content)))
	 (results '()))
    (doseq (entry (get parsed 'search))
      (let* ((id (get entry 'id))
	     (f (try (get wikid.map id) (property-ref id))))
	(do-choices (slot.value (getassocs entry))
	  (cond ((eq? (car slot.value) 'match))
		((vector? (cdr slot.value))
		 (store! f (car slot.value) (elts (cdr slot.value))))
		(else (store! f (car slot.value) (cdr slot.value)))))
	(if (test f 'label)
	    (store! f '%id `(,(get f 'label) ,(get f 'id) WIKIDATA))
	    (store! f '%id `(,(get f 'id) WIKIDATA)))
	(lineout "Made " f)
	(set! results (cons f results)))
      (and (pair? results) (reverse (->vector results))))))

(when (config 'optimize #t)
  (optimize! '{brico brico/indexing storage/flex storage/flexpools})
  (optimize!))
