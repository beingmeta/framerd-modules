;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'fdpkg)

(use-module '{fdweb texttools logger fileio})

(module-export! '{fdpkg/link!})

(define-init %loglevel %notify%)

(define (fdpkg/link! source (dest))
  (default! dest 'installed_modules)
  (when (symbol? dest)
    (if (config dest)
	(set! dest (config dest))
	(irritant dest "Invalid destination")))
  (unless (and (string? dest) (file-directory? dest))
    (irritant dest "Invalid destination"))
  (do-choices (file (getfiles source))
    (link-file (abspath file) (mkpath dest (basename file))))
  (do-choices (dir (getdirs source))
    (link-file (abspath dir) (mkpath dest (basename dir)))
    (link-module.scm dir)))

(define (link-module.scm submodule)
  (unless (file-exists? (mkpath submodule "module.scm"))
    (when(file-exists? (mkpath submodule (glom (basename submodule) ".scm")))
      (link-file (glom (basename submodule) ".scm")
		 (mkpath submodule "module.scm"))))
  (link-module.scm (getdirs submodule)))

