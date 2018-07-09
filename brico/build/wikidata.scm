;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'brico/build/wikidata)

(use-module '{texttools fdweb logger varconfig stringfmts optimize})
(use-module '{storage/flex storage/typeindex})
(use-module '{brico brico/indexing})

(module-export! '{wikidata-dir wikid.pool wikid.map 
		  wikid.index meta.index
		  wikid wikid/ingest
		  wikid/read wikid/readn wikid/fetch
		  wikid/commit})

(define %loglevel %info%)

(define wikidata-dir #f)
(define wikidata-default-dir (get-component "wikidata/"))

(define *epoch* "testing")
(varconfig! wikidata:epoch *epoch*)

(define block-size 10000)
(define merge-size 1000)

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

(define wikid.index #f)
(define wikid_en.index #f)
(define wikid_en_norm.index #f)
(define wikid_types.index #f)
(define wikid_has.index #f)

(define meta.index #f)

(define wikidata.index #f)

(define (set-wikidata-root! dir)
  (unless (equal? wikidata-dir dir)
    (set! wikid.pool
      (db/ref (mkpath dir "wikid.flexpool")
	      `#[type flexpool 
		 base @31C1/0 capacity #1gib partsize ,(* 4 #1mib)
		 create #t prefix "wikid/wiki" compression zstd
		 metadata 
		 #[adjuncts #[%words #[pool "words.flexpool" prefix "words" create #t]
			      %glosses #[pool "glosses.flexpool" prefix "glosses" create #t]
			      %norms #[pool "norms.flexpool" prefix "norms" create #t]
			      %links #[pool "links.flexpool" prefix "links" create #t]]]
		 adjopts #[compression zstd]]))
    (set! wikid.map
      (db/ref (mkpath dir "wikid.map")
	      `#[indextype memindex
		 preload ,(config 'wikibuild #f)
		 register #t
		 create #t]))

    (set! wikid.index
      (db/ref (mkpath dir "wikid.index")
	      `#[type hashindex size ,(* 64 #mib) register #t create #t]))
    (set! wikid_en.index
      (db/ref (mkpath dir "en.index")
	      `#[type hashindex size ,(* 32 #mib) register #t create #t
		 keyslot @?en]))
    (set! wikid_en_norm.index
      (db/ref (mkpath dir "en_norm.index")
	      `#[type hashindex size ,(* 16 #mib) register #t create #t
		 keyslot @?en_norm]))
    (set! wikid_types.index
      (db/ref (mkpath dir "types")
	      `#[type typeindex keyslot type 
		 register #t create #t]))
    (set! wikid_has.index
      (db/ref (mkpath dir "has")
	      `#[type typeindex keyslot has
		 register #t create #t]))

    (set! meta.index 
      (db/ref (mkpath (config 'bricosource) "meta.index")
	      #[type hashindex register #t create #t]))

    (set! wikidata.index
      (make-aggregate-index {wikid.index wikid_en.index wikid_en_norm.index 
			     wikid_types.index wikid_has.index}
			    #[register #t]))

    (set! wikidata-dir dir)))

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
		 (find-frames (if isprop meta.index wikid.index)
		   'wikid string))
		(frame
		 (try existing
		      (frame-create (if isprop brico.pool wikid.pool)
			'wikid string '%ID string 
			'uuid (tryif uuid uuid)
			'type (if isprop 'property 'entity)
			'source 'wikidata))))
	   (store! wikid.map string frame)
	   (index-frame wikid.index frame '{wikid type})
	   (when isprop (index-frame meta.index frame '{wikid type}))
	   frame))))

(define (getmodel string)
  (if (has-prefix string "http://www.wikidata.org/entity/")
      (wikid (strip-prefix string "http://www.wikidata.org/entity/"))
      string))

;;; Converting dump data

(define (->langmap map (into `#[]))
  (do-choices (assoc (getassocs map))
    (if (vector? (cdr assoc))
	(store! into (car assoc) (get (elts (cdr assoc)) 'value))
	(store! into (car assoc) (get (cdr assoc) 'value))))
  into)
(define (->sitelinks map (into `#[]))
  (do-choices (assoc (getassocs map))
    (if (vector? (cdr assoc))
	(store! into (car assoc) (get (elts (cdr assoc)) 'title))
	(store! into (car assoc) (get (cdr assoc) 'title))))
  into)

(define (expandvecs x) (if (vector? x) (elts x) x))

(define (copy-frame data into (index #f))
  (when (test data 'title) (store! into 'title (get data 'title)))
  (store! into 'type (string->symbol (upcase (get data 'type))))
  (store! into 'label (get (get (get data 'labels) 'en) 'value))
  (store! into 'norms (get (get (get data 'labels) 'en) 'value))
  (store! into 'words {(get (elts (get (get data 'aliases) 'en)) 'value)
		       (get into 'norms)})
  (store! into 'gloss (get (get (get data 'descriptions) 'en) 'value))
  (store! into '%id (list (pick-one (get into 'norm)) (get into 'wikid)))
  (store! into '%norms (->langmap (get data 'labels)))
  (store! into '%labels (->langmap (get data 'labels)))
  (store! into '%words (->langmap (get data 'aliases)))
  (store! into '%glosses (->langmap (get data 'descriptions)))
  (store! into 'links (->sitelinks (get data 'sitelinks)))
  (store! into 'modified (timestamp (get data 'modified)))
  (store! into 'revid (get data 'revid))
  (store! into '%id (list (pick-one (get into 'norms)) (get into 'wikid)))
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
	(when (exists? (getkeys context))
	  (add! into prop (cons value context))))))
  (store! into 'epoch *epoch*)
  (when index (index-frame index into 'type))
  (when index (index-string index into @?en #default 2))
  (when index (index-string index into @?en_norms #default 2))
  (when index (index-frame index into 'has (getkeys into)))
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
		  (temp (make-hashtable))
		  (results {})
		  (data))
  (default! limit (getopt opts 'limit block-size))
  (default! merge (getopt opts 'merge merge-size))
  (set! data (onerror (jsonparse (reader)) #f))
  (while (and data (< count limit))
    (unless (empty-string? data)
      (set+! results (wikid/ingest data temp))
      (set! count (1+ count))
      (when (zero? (remainder count merge))
	(index-merge! index temp)
	(set! temp (make-hashtable))))
    (set! data (onerror (jsonparse (reader)) #f)))
  (index-merge! index temp)
  results)

(define (wikid/readn reader index (opts #f))
  (let ((nthreads (getopt opts 'nthreads (config 'nthreads (rusage 'ncpus))))
	(threads {}))
    (if (and nthreads (> nthreads 1))
	(begin (dotimes (i nthreads)
		 (set+! threads (thread/call readloop reader index opts)))
	  (thread/wait! threads))
	(readloop reader index opts))))

(define (wikid/fetch id (index #f))
  (let* ((fetched (urlget (glom "https://www.wikidata.org/wiki/Special:EntityData/" id ".json")))
	 (parsed (jsonparse (get fetched '%content))))
    (wikid/ingest parsed index)))

(define (wikid/commit)
  (flex/commit! 
   {wikid.pool wikid.index meta.index wikid.map brico.pool
    (getvalues (poolctl wikid.pool 'adjuncts))})
  (commit))

;;;; Cycles

(define-init base-wikid-load (pool-load wikid.pool))
(define-init base-brico-load (pool-load brico.pool))
(define-init session-started #f)

(define (wikid/cycle r (opts #f))
  (let ((started (elapsed-time))
	(start-brico-load (pool-load brico.pool))
	(start-wikid-load (pool-load wikid.pool)))
    (unless session-started (set! session-started started))
    (wikid/readn r wikid.index opts)
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
