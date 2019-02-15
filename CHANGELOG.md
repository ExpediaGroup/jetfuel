# Changelog

## 1.0.0 (2019-01-09)

### Breaking Changes

* Refactored Maven groupId to `com.expediagroup`
* Refactored Java package to `com.expediagroup.jetfuel.*`

This affects anyone using this project as a dependency.  JAR usage should be backwards-compatible as the command-line arguments are unchanged.

### Features

* Improved CLI support, with usage text and better error messages
* Added validation for File Format & Compression configurations; 
invalid or unsupported combinations will be rejected immediately
* Improved documentation
* Improved unit test coverage

### Bug Fixes

* Fixed compression settings for various File Formats

## 0.6.0 (2018-12-03)

### Features

* Added Dynamic Partition Grouping
* Improved documentation

## 0.5.0 (2018-11-08)

### Features

* Improved documentation 

### Bug Fixes

* Fixed Parquet compression

## 0.4.0 (2018-10-22)

### Features

* Added `preFueling.dropTable` option with a default value of `false`.
* Refactored `jetfuel-core` to `jetfuel`
* Updated README
* Misc cleanup and static analysis fixes
