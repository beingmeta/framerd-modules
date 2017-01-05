;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'fdpkg)

(use-module '{fdweb texttools logger reflection fileio})

(module-export! '{fdpkg/link! fdpkg/info})

(define-init %loglevel %notify%)

(define (base-prefix? string prefix)
  (has-prefix (basename string) prefix))

(define (get-conflicts source dest overwrite)
  (set! overwrite (abspath overwrite))
  (try (try-choices (file (reject (choice (getfiles source) (getdirs source))
				  base-prefix? "_"))
	 (tryif (and (file-exists? file) (readlink file #t)
		     (not (has-prefix (readlink file #t) overwrite)))
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
      (when (and (readlink file #t)
		 (overlaps? (readlink file #t) source))
	(logwarn |RemovedReference|
	  "Removing the missing installation link for file " file)
	(remove-file file)))
    (do-choices (dir (difference (getdirs dest) added))
      (when (and (readlink dir #t)
		 (overlaps? (abspath (readlink dir #t)) source))
	(logwarn |RemovedReference|
	  "Removing the missing installation link for path " dir)
	(remove-file dir)))))

(define (link-module.scm submodule)
  (unless (file-exists? (mkpath submodule "module.scm"))
    (when(file-exists? (mkpath submodule (glom (basename submodule) ".scm")))
      (link-file (glom (basename submodule) ".scm")
		 (mkpath submodule "module.scm"))))
  (link-module.scm (getdirs submodule)))

;;; Getting package info

(define (fdpkg/info arg (field #f))
  (if (and field (string? arg) 
	   (file-exists? (mkpath arg (downcase field))))
      (trim-spaces (filestring (mkpath arg (downcase field))))
      (let ((base (findpackageinfo arg)))
	(cond ((fail? base)
	       (irritant arg |No package info|))
	      ((not field) base)
	      ((file-directory? (mkpath base (downcase field)))
	       (mkpath base (downcase field)))
	      ((file-exists? (mkpath base (downcase field)))
	       (trim-spaces (filestring (mkpath base (downcase field)))))))))

(define (follow-link path)
  (set! path (strip-suffix path "/"))
  (or (readlink path #t) path))

(define (findpackageinfo arg)
  (cond ((string? arg) (find.fdpkginfo arg))
	((and (symbol? arg) (get-module arg))
	 (findpackageinfo (get-module arg)))
	((symbol? arg) (irritant arg |Not A Module| findpackageinfo))
	((or (environment? arg) (module? arg) (table? arg))
	 (let ((paths (pick (strip-prefix (pickstrings (get arg '%moduleid))
					  "file:")
			    has-prefix "/")))
	   (cond ((fail? paths) 
		  (error |No file path| findpackageinfo
			 "Can't determine source file for " arg))
		 (else (try-choices (path paths)
			 (findpackageinfo path))))))
	(else (irritant arg |No package ref|))))

(define (find.fdpkginfo path)
  (set! path (follow-link path))
  (until (or (not path) (equal? path "/")
	     (file-directory? (mkpath path ".fdpkg")))
    (set! path (follow-link (dirname path))))
  (tryif (and path (file-directory? (mkpath path ".fdpkg")))
    (mkpath path ".fdpkg")))


