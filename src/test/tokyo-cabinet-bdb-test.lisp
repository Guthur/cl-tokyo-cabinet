;;;
;;; Copyright (C) 2008 Keith James. All rights reserved.
;;;
;;; This program is free software: you can redistribute it and/or modify
;;; it under the terms of the GNU General Public License as published by
;;; the Free Software Foundation, either version 3 of the License, or
;;; (at your option) any later version.
;;;
;;; This program is distributed in the hope that it will be useful,
;;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;; GNU General Public License for more details.
;;;
;;; You should have received a copy of the GNU General Public License
;;; along with this program.  If not, see <http://www.gnu.org/licenses/>.
;;;

(in-package :cl-tokyo-cabinet-test)

(in-suite cl-tokyo-cabinet-system:testsuite)

(test new-bdb
  (let ((db (make-instance 'tc-bdb)))
    (is-true (cffi:pointerp (tc::ptr-of db)))
    (dbm-delete db)))

(test dbm-open/bdb
  (let ((db (make-instance 'tc-bdb))
        (bdb-filespec (namestring (iou:make-tmp-pathname
                                   :basename "bdb" :type "db"
                                   :tmpdir (merge-pathnames "data")))))
    ;; Can't create a new DB in read-only mode
    (signals dbm-error
      (dbm-open db bdb-filespec :read :create))
    (dbm-open db bdb-filespec :write :create)
    (is-true (fad:file-exists-p bdb-filespec))
    (is-true (delete-file bdb-filespec))))

(test dbm-vanish/bdb
  (with-fixture bdb-100 ()
    (dbm-vanish db)
    (is (zerop (dbm-num-records db)))))

(test dbm-num-records/bdb
  (with-fixture bdb-100 ()
    (is (= 100 (dbm-num-records db)))
    (dbm-vanish db)
    (is (zerop (dbm-num-records db)))))

(test dbm-file-size/bdb
  (with-fixture bdb-100 ()
    (with-open-file (stream bdb-filespec :direction :input)
        (= (dbm-file-size db)
           (file-length stream)))))

(test dbm-get/bdb/string/string
  (with-fixture bdb-100 ()
    (is-true (loop
                for i from 0 below 100
                for key = (format nil "key-~a" i)
                for value = (format nil "value-~a" i)
                always (string= (dbm-get db key) value)))))

(test dbm-get/bdb/string/octets
  (with-fixture bdb-100 ()
    (is-true (loop
                for i from 0 below 100
                for key = (format nil "key-~a" i)
                for value = (format nil "value-~a" i)
                always (string= (gpu:make-sb-string
                                 (dbm-get db key :octets)) value)))))

(test dbm-get/bdb/string/bad-type
  (with-fixture bdb-100 ()
    (signals error
      (dbm-get db "key-0" :bad-type))))

(test dbm-put/bdb/string/string
  (with-fixture bdb-empty ()
    ;; Add one
    (is-true (dbm-put db "key-one" "value-one"))
    (is (string= "value-one" (dbm-get db "key-one")))
    ;; Keep
    (signals dbm-error
        (dbm-put db "key-one" "VALUE-TWO" :mode :keep))
    ;; Replace
    (is-true (dbm-put db "key-one" "VALUE-TWO" :mode :replace))
    (is (string= "VALUE-TWO" (dbm-get db "key-one")))
    ;; Concat
    (is-true (dbm-put db "key-one" "VALUE-THREE" :mode :concat))
    (is (string= "VALUE-TWOVALUE-THREE" (dbm-get db "key-one")))))

(test dbm-put/bdb/string/octets
  (with-fixture bdb-empty ()
    (let ((octets (make-array 10 :element-type '(unsigned-byte 8)
                              :initial-contents (loop
                                                   for c across "abcdefghij"
                                                   collect (char-code c)))))
      (is-true (dbm-put db "key-one" octets))
      (is (equalp octets (dbm-get db "key-one" :octets))))))

(test dbm-put/bdb/int32/octets
  (let ((db (make-instance 'tc-bdb))
        (bdb-filespec (namestring (iou:make-tmp-pathname
                                   :basename "bdb" :type "db"
                                   :tmpdir (merge-pathnames "data"))))
        (octets (make-array 10 :element-type '(unsigned-byte 8)
                              :initial-contents (loop
                                                   for c across "abcdefghij"
                                                   collect (char-code c)))))
    (is-true (set-comparator db :int32))
    (dbm-open db bdb-filespec :write :create)
    ;; Add one
    (is-true (dbm-put db 111 octets))
    (is (equalp octets (dbm-get db 111 :octets)))
    (dbm-close db)
    (delete-file bdb-filespec)))

(test dbm-put/bdb/int32/string
  (let ((db (make-instance 'tc-bdb))
        (bdb-filespec (namestring (iou:make-tmp-pathname
                                   :basename "bdb" :type "db"
                                   :tmpdir (merge-pathnames "data")))))
    (is-true (set-comparator db :int32))
    (dbm-open db bdb-filespec :write :create)
    ;; Add one
    (is-true (dbm-put db 111 "value-one"))
    (is (string= "value-one" (dbm-get db 111)))
    ;; Keep
    (signals dbm-error
        (dbm-put db 111 "VALUE-TWO" :mode :keep))
    ;; Replace
    (is-true (dbm-put db 111 "VALUE-TWO" :mode :replace))
    (is (string= "VALUE-TWO" (dbm-get db 111)))
    ;; Concat
    (is-true (dbm-put db 111 "VALUE-THREE" :mode :concat))
    (is (string= "VALUE-TWOVALUE-THREE" (dbm-get db 111)))
    (dbm-close db)
    (delete-file bdb-filespec)))

(test dbm-get/bdb/int32/string
  (let ((db (make-instance 'tc-bdb))
        (bdb-filespec (namestring (iou:make-tmp-pathname
                                   :basename "bdb" :type "db"
                                   :tmpdir (merge-pathnames "data")))))
    (is-true (set-comparator db :int32))
    (dbm-open db bdb-filespec :write :create)
    (loop
       for i from 0 below 100
       do (dbm-put db i (format nil "value-~a" i)))
    (is-true (loop
                for i from 0 below 100
                for value = (format nil "value-~a" i)
                always (string= (dbm-get db i) value)))
    (dbm-close db)
    (delete-file bdb-filespec)))

(test dbm-iter/bdb/int32/string
  (let ((db (make-instance 'tc-bdb))
        (bdb-filespec (namestring (iou:make-tmp-pathname
                                   :basename "bdb" :type "db"
                                   :tmpdir (merge-pathnames "data")))))
    (is-true (set-comparator db :int32))
    (dbm-open db bdb-filespec :write :create)
    (loop
       for i from 0 below 100
       do (dbm-put db i (format nil "value-~a" i)))
    (let ((iter (iter-open db)))
      (iter-first iter)
      (is-true (loop
                  for i from 0 below 100
                  always (prog1
                             (and (= i (iter-key iter :integer))
                                  (string= (format nil "value-~a" i)
                                           (iter-get iter)))
                           (iter-next iter))))
      (iter-close iter))
    (dbm-close db)
    (delete-file bdb-filespec)))