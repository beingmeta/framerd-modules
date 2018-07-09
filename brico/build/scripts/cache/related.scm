(load "brico.scm")

(use-module '{brico brico/lookup gnosys/getrelated mttools stringfmts})

;;; The output file

(define datadir (get-component "data"))

(define (get-baseoids-from-base base capacity)
  (choice base
	  (tryif (> capacity 0)
		 (get-baseoids-from-base (oid-plus base capacity)
					 (- capacity (* 1024 1024))))))
(define (get-baseoids-from-pool pool)
  (sorted (get-baseoids-from-base (pool-base pool) (pool-capacity pool))
	  oid-lo))

(define (open-outfile filename)
  (if (has-suffix filename ".table")
      (if (file-exists? filename)
	  (file->dtype filename)
	  (make-hashtable 2000000))
      (if (file-exists? filename)
	  (open-index filename)
	  (begin (make-hash-index
		  filename 16000000
		  (append (get-baseoids-from-pool brico-pool)
			  (get-baseoids-from-pool xbrico-pool)
			  (get-baseoids-from-pool names-pool)
			  (get-baseoids-from-pool places-pool))
		  #f
		  '(B40))
		 (open-index filename)))))

(define (doprefetch index oids done)
  (when done (commit) (clearcaches))
  (unless done
    (prefetch-keys! index oids)
    (getrelated/prefetch! (qc oids) #f)))

(define (main outfile pool-arg . pools)
  (let* ((index (open-outfile outfile))
	 (pool (use-pool pool-arg)))
    (config! 'appid (stringout "cacherelated " pool-arg))
    (do-choices-mt (f (pool-elts pool) (config 'nthreads 4)
		      prefetcher (config 'blocksize 10000))
      (message "Getting related to " f)
      (let ((related (getrelated f)))
	(add! index f related))
      (message "Got related to " f))
    (unless (null? pools)
      (apply chain outfile pools))))

(define (main outfile . pool-args)
  (let* ((index (open-outfile outfile)))
    (dolist (pool-arg pool-args)
      (let ((pool (use-pool pool-arg)))
	(config! 'appid (stringout "cacherelated " pool-arg))
	(do-choices-mt (f (pool-elts pool) (config 'nthreads 4)
			  (lambda (oids done)
			    (doprefetch index (qc oids) done))
			  (config 'blocksize 10000))
	  (unless (exists? (get index f))
	    (message "Getting related to " f)
	    (let ((related (getrelated f)))
	      (add! index f related))
	    (message "Got related to " f)))))))

(optimize! '{brico brico/lookup brico/dterms gnosys/getrelated})
(optimize!)

