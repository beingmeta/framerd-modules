;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'fdpkg)

(use-module '{fdweb texttools logger fileio})

(module-export! '{fdpkg/link!})

(define-init %loglevel %notify%)

(define (base-prefix? string prefix)
  (has-prefix (basename string) prefix))

(define (get-conflicts source dest overwrite)
  (set! overwrite (abspath overwrite))
  (try (try-choices (file (reject (choice (getfiles source) (getdirs source))
				  base-prefix? "_"))
	 (tryif (and (file-exists? file) (readlink file)
		     (not (has-prefix (readlink file) overwrite)))
	   file))))

(define (fdpkg/link! source (dest) (opts #[]))
  (default! dest 'installed_modules)
  (when (symbol? dest)
    (if (config dest)
	(set! dest (config dest))
	(irritant dest "Invalid destination")))
  (unless (and (string? dest) (file-directory? dest))
    (irritant dest "Invalid destination"))
  (let ((conflicts (tryif (not (getopt opts 'overwrite))
		     (get-conflicts source dest
				    (qc dest (getopt opts 'replaces)))))
	(added {}))
    (when (exists? conflicts)
      (irritant conflicts |PackageConflicts| fdpkg/link!
		"When linking " source " into " dest))
    (do-choices (file (reject (getfiles source) base-prefix? "_"))
      (link-file (abspath file) (mkpath dest (basename file)))
      (set+! added (mkpath dest (basename file))))
    (do-choices (dir (reject (getdirs source) base-prefix? "_"))
      (unless (file-exists? (mkpath dir "_fdpkg.ignore"))
	(link-file (abspath dir) (mkpath dest (basename dir)))
	(set+! added (mkpath dest (basename dir)))
	(link-module.scm dir)))
    (set! added (choice added (abspath added)))
    (do-choices (file (difference (getfiles dest) added))
      (when (and (readlink file)
		 (overlaps? (abspath (readlink file)) source))
	(logwarn |RemovedReference|
	  "Removing the missing installation link for file " file)
	(remove-file file)))
    (do-choices (dir (difference (getdirs dest) added))
      (when (and (readlink dir)
		 (overlaps? (abspath (readlink dir)) source))
	(logwarn |RemovedReference|
	  "Removing the missing installation link for path " dir)
	(remove-file dir)))))

(define (link-module.scm submodule)
  (unless (file-exists? (mkpath submodule "module.scm"))
    (when(file-exists? (mkpath submodule (glom (basename submodule) ".scm")))
      (link-file (glom (basename submodule) ".scm")
		 (mkpath submodule "module.scm"))))
  (link-module.scm (getdirs submodule)))

