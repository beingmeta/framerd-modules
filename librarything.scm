;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'librarything)

;;; Provides access to the LibraryThing API

(use-module '{fdweb varconfig texttools ezrecords parsetime logger})

(module-export! '{lt/call lt/minimize lt/nohistory lt/getwork})

(define-init %loglevel %notice%)

(define lt-api-root "https://www.librarything.com/services/rest/1.1/")
(define lt-key #"d231aa37c9b4f5d304a60a3d0ad1dad4")

(varconfig! lt:root lt-api-root)
(varconfig! lt:key lt-key)

(define (lt/call method . args)
  (let* ((url (apply scripturl
		     lt-api-root
		     "method" (glom "librarything.ck." method)
		     "apikey" lt-key
		     args))
	 (response (urlget url))
	 (parsed (xmlparse (get response '%content) '{data slotify}))
	 (ltml (get (xmlget parsed 'response) 'ltml))
	 (items (get ltml 'item)))
    (do-choices (item items)
      (let ((fields (get (get (get item 'commonknowledge) 'fieldlist) 'field)))
	(store! item 'fields (simplify-field fields))
	(store! item 'author (simplify-node (get item 'author)))
	(drop! item '{fieldlist %qname %attribs %attribids %xmltag commonknowledge})
	(do-choices (field (get item 'fields))
	  (store! item (string->lisp (get field 'name)) (get field 'value))))
      (drop! item 'commonknowledge))
    items))
(define (lt/getwork . args)
  (apply lt/get "getwork" args))

(define (lt/minimize item)
  (drop! item 'fields)
  item)
(define (lt/nohistory item)
  (do-choices (field (get item 'fields)) (drop! field 'history))
  item)


(define (simplify-field field)
  (drop! field '{%qname %attribs %attribids %xmltag})
  (store! field 'history (simplify-version (get (get field 'versionlist) 'version)))
  (drop! field 'versionlist)
  (store! field 'value (get (largest (get field 'history) 'date) 'facts))
  field)

(define (simplify-version version)
  (drop! version '{%qname %attribs %attribids %xmltag})
  (store! version 'date 
	  (try (timestamp (xmlcontent (get version 'date)))
	       (timestamp (->number (get (get version 'date) 'timestamp)))))
  (store! version 'facts (simplify-string (get (get version 'factlist) 'fact)))
  (store! version 'person (simplify-person (get version 'person)))
  (drop! version 'factlist)
  version)

(define (simplify-person person)
  (drop! person '{%qname %attribs %attribids %xmltag})
  person)

(define (simplify-string string)
  (if (string? string)
      (stdspace (decode-entities (textsubst string #{"&lt;![CDATA[" "<![CDATA[" "]]&gt;" "]]>"} "")))
      string))

(define (simplify-node node)
  (drop! node '{%qname %attribs %attribids %xmltag})
  node)
