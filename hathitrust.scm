;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'hathitrust)

;;; Provides access to the Hathitrust API

(use-module '{fdweb varconfig texttools ezrecords parsetime logger})

(module-export! '{ht/get ht/full ht/brief})

(define-init %loglevel %notice%)

(define hathitrust-api-root "https://catalog.hathitrust.org/api/volumes/")

(varconfig! ht:root hathitrust-api-root)

(define (ht/get idtype id (brief #f))
  (let* ((url (glom hathitrust-api-root 
		(if brief "/brief/" "/full/")
		(downcase idtype) "/" id ".json"))
	 (response (urlget url))
	 (parsed (jsonparse (get response '%content)))
	 (recordmap (get parsed 'records))
	 (records (get recordmap (getkeys recordmap)))
	 (items (elts (get parsed 'items))))
    (do-choices (record records)
      (do-choices (key (getkeys record))
	(let ((value (get record key)))
	  (when (vector? value)
	    (store! record key (elts value)))))
      (if (test record 'marc-xml)
	  (store! record 'marc-xml
		  (simplify-xml
		   (reject (elts (xmlparse (get record 'marc-xml)
					   '{data slotify}))
			   string?)))))
    (do-choices (item items)
      (when (test item 'fromrecord)
	(store! item 'fromrecord (->number (get item 'fromrecord))))
      (when (test recordmap (->number (get item 'fromrecord)))
	(store! item 'fromrecord
		(get recordmap (->number (get item 'fromrecord))))))
    (store! parsed 'items items)
    parsed))

(define (ht/full idtype id) (ht/get idtype id #f))
(define (ht/brief idtype id) (ht/get idtype id #f))

(define (simplify-xml node)
  (drop! node '{%xmltag %attribs %attribids %qname})
  (do-choices (key (getkeys node))
    (let* ((values (get node key))
	   (descend (if (ambiguous? values)
			(reject values string?)
			(if (or (pair? values) (vector? values))
			    (reject (elts values) string?)
			    (tryif (table? values) values)))))
      (do-choices (value descend)
	(when (slotmap? value) (simplify-xml value)))))
  node)
