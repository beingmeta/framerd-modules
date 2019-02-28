(in-module 'read-delimited)

;;; TODO
;;; - use GETOPT

(module-export! '{read-delimited read-delimited-file})

(load "strings.scm")

(use-module 'texttools)

(define (convert-cell val)
  (if (string? val)
      (if (and (has-prefix val "\"")  (has-suffix val "\""))
          (convert-cell (subseq val 1 -1))
          (if (compound-string? val)
              val
              (if (empty-string? val)
                  val
                  (string->lisp val))))
      val))

(define (read-delimited string (cols #f) (cellsep ","))
  (let* ((rows (segment string "\n"))
         (cols (or cols (map convert-cell (segment (first rows) cellsep)))))
    (for-choices (row (elts rows 1))    ; use STRIM-TRIM-LEFT
                 (tryif (not (or (empty-string? row)
                                 (has-prefix row "#")
                                 (has-prefix row ";")))
                        (let ((row (map convert-cell (segment row cellsep))))
                          (let ((f (frame-create #f)))
                            (dotimes (i (length row))
                                     (store! f (elt cols i) (elt row i)))
                            f))))))

(define (read-delimited-file file (cols #f) (cellsep ","))
  (read-delimited (filestring file) cols cellsep))

(define (read-delimited-line line) #f)

;;; Return a list from FILE
(define (file->list file)
  (string-split (filestring file) #\newline))

;;; Remove comments from lines
(define (uncomment lines (char #\#))
  (let ((string (->string char)))
    (remove-if (lambda (line) (has-prefix line string))
               lines)))

;;; Return cleaned up list from FILE
(define (file->list/clean file)
  (uncomment (clean-up (file->list file))))

;;; Append newline to string
(define (string-append-newline string)
  (string-append string (->string #\newline)))

;;; Return a clean filestring from FILE
(define (filestring/clean file)
  (let ((input (filestring file)))
    (apply string-append
           (map string-append-newline
                (file->list/clean file)))))
