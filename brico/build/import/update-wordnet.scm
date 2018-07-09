(use-module '{texttools})
(use-module '{logger varconfig optimize stringfmts})

(config! 'cachelevel 2)
(config! 'dbloglevel %info%)
(config! 'bricosource (get-component "new"))
(config! 'brico:wordnet @1/46074)

(use-module '{storage/flex})
(use-module '{brico brico/indexing brico/build/wordnet})

(define en @1/2c1c7)
(define wordnet-dir (abspath (get-component "wordnet/WordNet-3.0/")))
(varconfig! wordnet wordnet-dir)

(define %loglevel %warn%)

(define (main reln)
  (poolctl brico.pool 'readonly #f)
  (indexctl core.index 'readonly #f)
  (indexctl wordnet.index 'readonly #f)
  (indexctl wordforms.index 'readonly #f)
  (link-release! (mkpath wordnet-dir "dict/index.sense") 'wn30)
  (when (exists? (check-release-links 'wn30))
    (error |InconsistentLinks|))
  (import-synsets (mkpath wordnet-dir "dict/data.noun"))
  (import-synsets (mkpath wordnet-dir "dict/data.verb"))
  (import-synsets (mkpath wordnet-dir "dict/data.adj"))
  (import-synsets (mkpath wordnet-dir "dict/data.adv"))
  (fix-wordform (difference
		 (find-frames wordnet.index 'type 'wordform)
		 (find-frames wordnet.index 'has 'sensenum)))
  (fix-wnsplit (find-frames wordnet.index 'has 'wnsplit)))

(when (config 'optimize #t)
  (optimize! '{brico brico/indexing brico/wordnet}))
