;;; -*- Mode: Scheme; -*-

(use-module '{logger varconfig fifo engine storage/flex stringfmts ezrecords})

(config! 'cachelevel 2)

(define indir (config 'indir (abspath "orig/")))
(define outdir (config 'outdir (abspath "output/")))

(define brico-pool-names
  {"brico.pool" "xbrico.pool" "names.pool" "places.pool"})

(define (check-dirs)
  (unless (file-directory? indir)
    (logpanic |BadInputDir|
      "The specified input path " (write indir) " isn't a directory"))
  (unless (file-directory? outdir)
    (logpanic |BadOutputDir|
      "The specified output path " (write outdir) " isn't a directory")))
(when (config 'checkdirs #t config:boolean) (check-dirs))

;;(define misc-slotids (file->dtype (mkpath data-dir "miscslots.dtype")))

(config! 'bricosource indir)

(use-module '{brico brico/indexing})
(use-module '{brico brico/indexing mttools trackrefs optimize tinygis})
(use-module '{logger varconfig})

(defrecord (langinfo)
  language id norm frag cue gloss)

(define lang-slots
  (let ((table (make-hashtable)))
    (do-choices (lang (pickoids all-languages))
      (store! table {lang (get lang 'key)}
	      (cons-langinfo lang (get lang 'key) 
			     (get norm-map lang)
			     (get frag-map lang)
			     (get indicator-map lang)
			     (get gloss-map lang))))))

(define (make-threadindex base)
  (let ((tmp (frame-create #f)))
    (do-choices (slotid (getkeys base))
      (store! tmp slotid (make-hashtable)))
    tmp))

(define (threadindex/merge! into from)
  (do-choices (slotid (getkeys from))
    (index/merge! (try (get into slotid) (get into '%default)) 
		  (get from slotid))))

(define dontindex (choice (?? 'source @1/1)))

(define (target-file name) (mkpath outdir name))

(define (target-index filename (opts #f) (size) (keyslot))
  (default! size (getopt opts 'size (config 'INDEXSIZE (* 8 #mib))))
  (default! keyslot (getopt opts 'keyslot (config 'keyslot #f)))
  (when (not size) (set! size (* 8 #mib)))
  (unless (position #\/ filename) 
    (set! filename (mkpath outdir filename)))
  (cond ((and (file-exists? filename) (not (config 'REBUILD #f config:boolean)))
	 (open-index filename `(#[register ,(getopt opts 'register #t)] . ,opts)))
	(else (when (file-exists? filename) 
		(logwarn |ReplacingFile| 
		  (write filename) ", backup in " 
		  (write  (glom filename ".bak")))
		(move-file filename (glom filename ".bak")))
	      (make-index filename
		`(#[type hashindex size ,size keyslot ,keyslot] . ,opts))
	      (lognotice |NewIndex| "Making new file index " filename)
	      (open-index filename `(#[register ,(getopt opts 'register #t)] . ,opts)))))
