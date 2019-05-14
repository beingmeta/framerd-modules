;;; -*- Mode: Scheme; -*-

(in-module 'elasticsearch)

(use-module '{fdweb logger texttools jsonout varconfig})

(module-export! '{req->json 
		  elastic/new elastic/upload elastic/delete
		  elastic/search elastic/search/x})

(define *endpoint* #f)
(varconfig! elastic:endpoint *endpoint*)

;;; Interface to elastic search

(define (req->json s)
  (if (response/ok? s)
      (jsonparse (get s '%content))
      s))

(define (elastic/new name (spec #f) (opts #f) (endpoint) (url))
  (default! endpoint (getopt opts 'endpoint *endpoint*))
  (set! url (mkpath endpoint name))
  (if (response/ok? (urlget url))
      url
      (let* ((response (urlput url
			       (if spec (json->string spec) "")
			       (if spec "application/json" ""))))
	(if (response/ok? response)
	    (if (getopt opts 'verbose) 
		(jsonparse (get response '%content))
		url)
	    response))))

(define (elastic/delete endpoint (opts #f))
  (req->json (urlput endpoint "" "text/plain" [method "DELETE"])))

(define (elastic/upload endpoint doc (opts #f))
  (when (string? doc)
    (set! doc [content doc content-type "text/plain"]))
  (let* ((id (try (get doc 'docid) (getopt opts 'newid #f)))
	 (url (if id
		  (mkpath (mkpath endpoint "_doc") id)
		  (mkpath endpoint "_doc"))))
    (req->json (urlput url (json->string doc) "application/json"
		       [method (if id "PUT" "POST")]))))

(define (elastic/search/x endpoint query (opts #f))
  (let* ((size (if (number? opts) opts (getopt opts 'size (getopt opts 'maxrank))))
	 (url (mkpath endpoint "_search"))
	 (base (if (table? query) query
		   (frame-create #f
		     (getopt opts 'matchop 'match)
		     (frame-create #f
		       (getopt opts'matchslot 'norm)
		       query))))
	 (query (frame-create #f
		  'query base
		  'size (tryif size size))))
    (req->json (urlput url (json->string query) "application/json"
		       [method "GET"]))))

(define (elastic/search endpoint query (opts #f))
  (let ((result (elastic/search/x endpoint query opts)))
    (forseq (hit (get (get result 'hits) 'hits))
      (frame-create hit 
	'%id (get (parse-arg (get hit '_id)) 'text)
	'oid (parse-arg (get hit '_id))))))

