(load-component "indexbase.scm")

(defambda (prefetcher index keys done)
  (when done (commit) (clearcaches))
  (unless done (prefetch-keys! index keys)))

(define (compute-absfreq-table index table)
  (do-choices-mt (key (getkeys index) (config 'nthreads 4)
		      (lambda (keys done) (prefetcher index keys done))
		      (config 'blocksize 100000))
    (hashtable-increment! table (cdr key)
      (choice-size (get index key)))))

(define (main indexfile writefile)
  (let ((table (make-hashtable 4000000)))
    (unless (config 'nowordforms #f)
      (use-index (get-component "data/wordforms.index"))
      (do-choices-mt (f (?? 'type 'wordform) (config 'nthreads 4)
			mt/fetchoids (config 'blocksize 20000))
	(hashtable-increment! table (get f 'concept)
	  (* (try (get f 'freq) 1) (try (get f 'freq) 1)))
	(hashtable-increment! table
	    (vector (get f 'concept) (get f 'language) (get f 'word))
	  (try (get f 'freq) 1))))
    (compute-absfreq-table (open-index indexfile) table)
    (let ((newtable (make-hashtable (table-size table))))
      (notify "Trimming frequency table with " (table-size table) " items")
      (do-choices (key (getkeys table))
	(when (> (get table key) 1)
	  (store! newtable key (get table key))))
      (notify "Writing frequency table with " (table-size newtable) " items")
      (dtype->file newtable writefile 500000))))

(optimize!)



