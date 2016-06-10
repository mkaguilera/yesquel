/* This file is from SQLite, with modifications.
   The modifications are indicated with comments preceded by "YESQUEL CH",
   in addition to
     - minor changes to compile with C++
     - some variable changes from 32 to 64 bits
     - removal of autorization code (as if compiled with SQLITE_OMIT_AUTHORIZATION)
*/

#define SQLITE_CORE 1
#define SQLITE_AMALGAMATION 1
#ifndef SQLITE_PRIVATE
# define SQLITE_PRIVATE static
#endif
#ifndef SQLITE_API
# define SQLITE_API
#endif

#include "options.h"

#define SQLITE_OMIT_AUTOVACUUM
#ifdef DEBUGLOG
#define LOG(format,...) fprintf(stderr, "%s:%d:" format "\n", __FUNCTION__, __LINE__, ##__VA_ARGS__)
#else
#define LOG(format,...)
#endif
#define TRACER LOG("tracer")

#include "kvinterface.h"

/************** Begin file sqliteInt.h ***************************************/
/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Internal interface definitions for SQLite.
**
*/
#ifndef _SQLITEINT_H_
#define _SQLITEINT_H_

#ifdef __cplusplus
extern "C" {
#endif


/*
** These #defines should enable >2GB file support on POSIX if the
** underlying operating system supports it.  If the OS lacks
** large file support, or if the OS is windows, these should be no-ops.
**
** Ticket #2739:  The _LARGEFILE_SOURCE macro must appear before any
** system #includes.  Hence, this block of code must be the very first
** code in all source files.
**
** Large file support can be disabled using the -DSQLITE_DISABLE_LFS switch
** on the compiler command line.  This is necessary if you are compiling
** on a recent machine (ex: Red Hat 7.2) but you want your code to work
** on an older machine (ex: Red Hat 6.0).  If you compile on Red Hat 7.2
** without this option, LFS is enable.  But LFS does not exist in the kernel
** in Red Hat 6.0, so the code won't work.  Hence, for maximum binary
** portability you should omit LFS.
**
** Similar is true for Mac OS X.  LFS is only supported on Mac OS X 9 and later.
*/
#ifndef SQLITE_DISABLE_LFS
# define _LARGE_FILE       1
# ifndef _FILE_OFFSET_BITS
#   define _FILE_OFFSET_BITS 64
# endif
# define _LARGEFILE_SOURCE 1
#endif

/*
** Include the configuration header output by 'configure' if we're using the
** autoconf-based build
*/
#ifdef _HAVE_SQLITE_CONFIG_H
#include "config.h"
#endif

/************** Include sqliteLimit.h in the middle of sqliteInt.h ***********/
/************** Begin file sqliteLimit.h *************************************/
/*
** 2007 May 7
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** 
** This file defines various limits of what SQLite can process.
*/

/*
** The maximum length of a TEXT or BLOB in bytes.   This also
** limits the size of a row in a table or index.
**
** The hard limit is the ability of a 32-bit signed integer
** to count the size: 2^31-1 or 2147483647.
*/
#ifndef SQLITE_MAX_LENGTH
# define SQLITE_MAX_LENGTH 1000000000
#endif

/*
** This is the maximum number of
**
**    * Columns in a table
**    * Columns in an index
**    * Columns in a view
**    * Terms in the SET clause of an UPDATE statement
**    * Terms in the result set of a SELECT statement
**    * Terms in the GROUP BY or ORDER BY clauses of a SELECT statement.
**    * Terms in the VALUES clause of an INSERT statement
**
** The hard upper limit here is 32676.  Most database people will
** tell you that in a well-normalized database, you usually should
** not have more than a dozen or so columns in any table.  And if
** that is the case, there is no point in having more than a few
** dozen values in any of the other situations described above.
*/
#ifndef SQLITE_MAX_COLUMN
# define SQLITE_MAX_COLUMN 512
#endif

/*
** The maximum length of a single SQL statement in bytes.
**
** It used to be the case that setting this value to zero would
** turn the limit off.  That is no longer true.  It is not possible
** to turn this limit off.
*/
#ifndef SQLITE_MAX_SQL_LENGTH
# define SQLITE_MAX_SQL_LENGTH 1000000000
#endif

/*
** The maximum depth of an expression tree. This is limited to 
** some extent by SQLITE_MAX_SQL_LENGTH. But sometime you might 
** want to place more severe limits on the complexity of an 
** expression.
**
** A value of 0 used to mean that the limit was not enforced.
** But that is no longer true.  The limit is now strictly enforced
** at all times.
*/
#ifndef SQLITE_MAX_EXPR_DEPTH
# define SQLITE_MAX_EXPR_DEPTH 1000
#endif

/*
** The maximum number of terms in a compound SELECT statement.
** The code generator for compound SELECT statements does one
** level of recursion for each term.  A stack overflow can result
** if the number of terms is too large.  In practice, most SQL
** never has more than 3 or 4 terms.  Use a value of 0 to disable
** any limit on the number of terms in a compount SELECT.
*/
#ifndef SQLITE_MAX_COMPOUND_SELECT
# define SQLITE_MAX_COMPOUND_SELECT 500
#endif

/*
** The maximum number of opcodes in a VDBE program.
** Not currently enforced.
*/
#ifndef SQLITE_MAX_VDBE_OP
# define SQLITE_MAX_VDBE_OP 25000
#endif

/*
** The maximum number of arguments to an SQL function.
*/
#ifndef SQLITE_MAX_FUNCTION_ARG
# define SQLITE_MAX_FUNCTION_ARG 127
#endif

/*
** The maximum number of in-memory pages to use for the main database
** table and for temporary tables.  The SQLITE_DEFAULT_CACHE_SIZE
*/
#ifndef SQLITE_DEFAULT_CACHE_SIZE
# define SQLITE_DEFAULT_CACHE_SIZE  2000
#endif
#ifndef SQLITE_DEFAULT_TEMP_CACHE_SIZE
# define SQLITE_DEFAULT_TEMP_CACHE_SIZE  500
#endif

/*
** The default number of frames to accumulate in the log file before
** checkpointing the database in WAL mode.
*/
#ifndef SQLITE_DEFAULT_WAL_AUTOCHECKPOINT
# define SQLITE_DEFAULT_WAL_AUTOCHECKPOINT  1000
#endif

/*
** The maximum number of attached databases.  This must be between 0
** and 62.  The upper bound on 62 is because a 64-bit integer bitmap
** is used internally to track attached databases.
*/
#ifndef SQLITE_MAX_ATTACHED
# define SQLITE_MAX_ATTACHED 10
#endif


/*
** The maximum value of a ?nnn wildcard that the parser will accept.
*/
#ifndef SQLITE_MAX_VARIABLE_NUMBER
# define SQLITE_MAX_VARIABLE_NUMBER 999
#endif

/* Maximum page size.  The upper bound on this value is 65536.  This a limit
** imposed by the use of 16-bit offsets within each page.
**
** Earlier versions of SQLite allowed the user to change this value at
** compile time. This is no longer permitted, on the grounds that it creates
** a library that is technically incompatible with an SQLite library 
** compiled with a different limit. If a process operating on a database 
** with a page-size of 65536 bytes crashes, then an instance of SQLite 
** compiled with the default page-size limit will not be able to rollback 
** the aborted transaction. This could lead to database corruption.
*/
#ifdef SQLITE_MAX_PAGE_SIZE
# undef SQLITE_MAX_PAGE_SIZE
#endif
#define SQLITE_MAX_PAGE_SIZE 65536


/*
** The default size of a database page.
*/
#ifndef SQLITE_DEFAULT_PAGE_SIZE
# define SQLITE_DEFAULT_PAGE_SIZE 1024
#endif
#if SQLITE_DEFAULT_PAGE_SIZE>SQLITE_MAX_PAGE_SIZE
# undef SQLITE_DEFAULT_PAGE_SIZE
# define SQLITE_DEFAULT_PAGE_SIZE SQLITE_MAX_PAGE_SIZE
#endif

/*
** Ordinarily, if no value is explicitly provided, SQLite creates databases
** with page size SQLITE_DEFAULT_PAGE_SIZE. However, based on certain
** device characteristics (sector-size and atomic write() support),
** SQLite may choose a larger value. This constant is the maximum value
** SQLite will choose on its own.
*/
#ifndef SQLITE_MAX_DEFAULT_PAGE_SIZE
# define SQLITE_MAX_DEFAULT_PAGE_SIZE 8192
#endif
#if SQLITE_MAX_DEFAULT_PAGE_SIZE>SQLITE_MAX_PAGE_SIZE
# undef SQLITE_MAX_DEFAULT_PAGE_SIZE
# define SQLITE_MAX_DEFAULT_PAGE_SIZE SQLITE_MAX_PAGE_SIZE
#endif


/*
** Maximum number of pages in one database file.
**
** This is really just the default value for the max_page_count pragma.
** This value can be lowered (or raised) at run-time using that the
** max_page_count macro.
*/
#ifndef SQLITE_MAX_PAGE_COUNT
# define SQLITE_MAX_PAGE_COUNT 1073741823
#endif

/*
** Maximum length (in bytes) of the pattern in a LIKE or GLOB
** operator.
*/
#ifndef SQLITE_MAX_LIKE_PATTERN_LENGTH
# define SQLITE_MAX_LIKE_PATTERN_LENGTH 50000
#endif

/*
** Maximum depth of recursion for triggers.
**
** A value of 1 means that a trigger program will not be able to itself
** fire any triggers. A value of 0 means that no trigger programs at all 
** may be executed.
*/
#ifndef SQLITE_MAX_TRIGGER_DEPTH
# define SQLITE_MAX_TRIGGER_DEPTH 1000
#endif

/************** End of sqliteLimit.h *****************************************/
/************** Continuing where we left off in sqliteInt.h ******************/

/* Disable nuisance warnings on Borland compilers */
#if defined(__BORLANDC__)
#pragma warn -rch /* unreachable code */
#pragma warn -ccc /* Condition is always true or false */
#pragma warn -aus /* Assigned value is never used */
#pragma warn -csu /* Comparing signed and unsigned */
#pragma warn -spa /* Suspicious pointer arithmetic */
#endif

/* Needed for various definitions... */
#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif

/*
** Include standard header files as necessary
*/
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif
#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

/*
** The number of samples of an index that SQLite takes in order to 
** construct a histogram of the table content when running ANALYZE
** and with SQLITE_ENABLE_STAT2
*/
#define SQLITE_INDEX_SAMPLES 10

/*
** The following macros are used to cast pointers to integers and
** integers to pointers.  The way you do this varies from one compiler
** to the next, so we have developed the following set of #if statements
** to generate appropriate macros for a wide range of compilers.
**
** The correct "ANSI" way to do this is to use the intptr_t type. 
** Unfortunately, that typedef is not available on all compilers, or
** if it is available, it requires an #include of specific headers
** that vary from one machine to the next.
**
** Ticket #3860:  The llvm-gcc-4.2 compiler from Apple chokes on
** the ((void*)&((char*)0)[X]) construct.  But MSVC chokes on ((void*)(X)).
** So we have to define the macros in different ways depending on the
** compiler.
*/
#if defined(__PTRDIFF_TYPE__)  /* This case should work for GCC */
# define SQLITE_INT_TO_PTR(X)  ((void*)(__PTRDIFF_TYPE__)(X))
# define SQLITE_PTR_TO_INT(X)  ((int)(__PTRDIFF_TYPE__)(X))
#elif !defined(__GNUC__)       /* Works for compilers other than LLVM */
# define SQLITE_INT_TO_PTR(X)  ((void*)&((char*)0)[X])
# define SQLITE_PTR_TO_INT(X)  ((int)(((char*)X)-(char*)0))
#elif defined(HAVE_STDINT_H)   /* Use this case if we have ANSI headers */
# define SQLITE_INT_TO_PTR(X)  ((void*)(intptr_t)(X))
# define SQLITE_PTR_TO_INT(X)  ((int)(intptr_t)(X))
#else                          /* Generates a warning - but it always works */
# define SQLITE_INT_TO_PTR(X)  ((void*)(X))
# define SQLITE_PTR_TO_INT(X)  ((int)(X))
#endif

/*
** The SQLITE_THREADSAFE macro must be defined as 0, 1, or 2.
** 0 means mutexes are permanently disable and the library is never
** threadsafe.  1 means the library is serialized which is the highest
** level of threadsafety.  2 means the libary is multithreaded - multiple
** threads can use SQLite as long as no two threads try to use the same
** database connection at the same time.
**
** Older versions of SQLite used an optional THREADSAFE macro.
** We support that for legacy.
*/
#if !defined(SQLITE_THREADSAFE)
#if defined(THREADSAFE)
# define SQLITE_THREADSAFE THREADSAFE
#else
# define SQLITE_THREADSAFE 2 /* IMP: R-07272-22309 */
#endif
#endif

/*
** The SQLITE_DEFAULT_MEMSTATUS macro must be defined as either 0 or 1.
** It determines whether or not the features related to 
** SQLITE_CONFIG_MEMSTATUS are available by default or not. This value can
** be overridden at runtime using the sqlite3_config() API.
*/
#if !defined(SQLITE_DEFAULT_MEMSTATUS)
# define SQLITE_DEFAULT_MEMSTATUS 1
#endif

/*
** Exactly one of the following macros must be defined in order to
** specify which memory allocation subsystem to use.
**
**     SQLITE_SYSTEM_MALLOC          // Use normal system malloc()
**     SQLITE_MEMDEBUG               // Debugging version of system malloc()
**
** (Historical note:  There used to be several other options, but we've
** pared it down to just these two.)
**
** If none of the above are defined, then set SQLITE_SYSTEM_MALLOC as
** the default.
*/
#if defined(SQLITE_SYSTEM_MALLOC)+defined(SQLITE_MEMDEBUG)>1
# error "At most one of the following compile-time configuration options\
 is allows: SQLITE_SYSTEM_MALLOC, SQLITE_MEMDEBUG"
#endif
#if defined(SQLITE_SYSTEM_MALLOC)+defined(SQLITE_MEMDEBUG)==0
# define SQLITE_SYSTEM_MALLOC 1
#endif

/*
** If SQLITE_MALLOC_SOFT_LIMIT is not zero, then try to keep the
** sizes of memory allocations below this value where possible.
*/
#if !defined(SQLITE_MALLOC_SOFT_LIMIT)
# define SQLITE_MALLOC_SOFT_LIMIT 1024
#endif

/*
** We need to define _XOPEN_SOURCE as follows in order to enable
** recursive mutexes on most Unix systems.  But Mac OS X is different.
** The _XOPEN_SOURCE define causes problems for Mac OS X we are told,
** so it is omitted there.  See ticket #2673.
**
** Later we learn that _XOPEN_SOURCE is poorly or incorrectly
** implemented on some systems.  So we avoid defining it at all
** if it is already defined or if it is unneeded because we are
** not doing a threadsafe build.  Ticket #2681.
**
** See also ticket #2741.
*/
#if !defined(_XOPEN_SOURCE) && !defined(__DARWIN__) && !defined(__APPLE__) && SQLITE_THREADSAFE
#  define _XOPEN_SOURCE 500  /* Needed to enable pthread recursive mutexes */
#endif

/*
** The TCL headers are only needed when compiling the TCL bindings.
*/
#if defined(SQLITE_TCL) || defined(TCLSH)
//# include <tcl.h>
#endif

/*
** Many people are failing to set -DNDEBUG=1 when compiling SQLite.
** Setting NDEBUG makes the code smaller and run faster.  So the following
** lines are added to automatically set NDEBUG unless the -DSQLITE_DEBUG=1
** option is set.  Thus NDEBUG becomes an opt-in rather than an opt-out
** feature.
*/

#ifdef NODEBUG
# ifndef NDEBUG
#  define NDEBUG
# endif
#else
# define SQLITE_DEBUG 1
#endif

/*
** The testcase() macro is used to aid in coverage testing.  When 
** doing coverage testing, the condition inside the argument to
** testcase() must be evaluated both true and false in order to
** get full branch coverage.  The testcase() macro is inserted
** to help ensure adequate test coverage in places where simple
** condition/decision coverage is inadequate.  For example, testcase()
** can be used to make sure boundary values are tested.  For
** bitmask tests, testcase() can be used to make sure each bit
** is significant and used at least once.  On switch statements
** where multiple cases go to the same block of code, testcase()
** can insure that all cases are evaluated.
**
*/
#ifdef SQLITE_COVERAGE_TEST
SQLITE_PRIVATE   void sqlite3Coverage(int);
# define testcase(X)  if( X ){ sqlite3Coverage(__LINE__); }
#else
# define testcase(X)
#endif

/*
** The TESTONLY macro is used to enclose variable declarations or
** other bits of code that are needed to support the arguments
** within testcase() and assert() macros.
*/
#if !defined(NDEBUG) || defined(SQLITE_COVERAGE_TEST)
# define TESTONLY(X)  X
#else
# define TESTONLY(X)
#endif

/*
** Sometimes we need a small amount of code such as a variable initialization
** to setup for a later assert() statement.  We do not want this code to
** appear when assert() is disabled.  The following macro is therefore
** used to contain that setup code.  The "VVA" acronym stands for
** "Verification, Validation, and Accreditation".  In other words, the
** code within VVA_ONLY() will only run during verification processes.
*/
#ifndef NDEBUG
# define VVA_ONLY(X)  X
#else
# define VVA_ONLY(X)
#endif

/*
** The ALWAYS and NEVER macros surround boolean expressions which 
** are intended to always be true or false, respectively.  Such
** expressions could be omitted from the code completely.  But they
** are included in a few cases in order to enhance the resilience
** of SQLite to unexpected behavior - to make the code "self-healing"
** or "ductile" rather than being "brittle" and crashing at the first
** hint of unplanned behavior.
**
** In other words, ALWAYS and NEVER are added for defensive code.
**
** When doing coverage testing ALWAYS and NEVER are hard-coded to
** be true and false so that the unreachable code then specify will
** not be counted as untested code.
*/
#if defined(SQLITE_COVERAGE_TEST)
# define ALWAYS(X)      (1)
# define NEVER(X)       (0)
#elif !defined(NDEBUG)
# define ALWAYS(X)      ((X)?1:(assert(0),0))
# define NEVER(X)       ((X)?(assert(0),1):0)
#else
# define ALWAYS(X)      (X)
# define NEVER(X)       (X)
#endif

/*
** Return true (non-zero) if the input is a integer that is too large
** to fit in 32-bits.  This macro is used inside of various testcase()
** macros to verify that we have tested SQLite for large-file support.
*/
#define IS_BIG_INT(X)  (((X)&~(i64)0xffffffff)!=0)

/*
** The macro unlikely() is a hint that surrounds a boolean
** expression that is usually false.  Macro likely() surrounds
** a boolean expression that is usually true.  GCC is able to
** use these hints to generate better code, sometimes.
*/
#if defined(__GNUC__) && 0
# define likely(X)    __builtin_expect((X),1)
# define unlikely(X)  __builtin_expect((X),0)
#else
# define likely(X)    !!(X)
# define unlikely(X)  !!(X)
#endif

/************** Include sqlite3.h in the middle of sqliteInt.h ***************/
/************** Begin file sqlite3.h *****************************************/
/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines the interface that the SQLite library
** presents to client programs.  If a C-function, structure, datatype,
** or constant definition does not appear in this file, then it is
** not a published API of SQLite, is subject to change without
** notice, and should not be referenced by programs that use SQLite.
**
** Some of the definitions that are in this file are marked as
** "experimental".  Experimental interfaces are normally new
** features recently added to SQLite.  We do not anticipate changes
** to experimental interfaces but reserve the right to make minor changes
** if experience from use "in the wild" suggest such changes are prudent.
**
** The official C-language API documentation for SQLite is derived
** from comments in this file.  This file is the authoritative source
** on how SQLite interfaces are suppose to operate.
**
** The name of this file under configuration management is "sqlite.h.in".
** The makefile makes some minor changes to this file (such as inserting
** the version number) and changes its name to "sqlite3.h" as
** part of the build process.
*/
#ifndef _SQLITE3_H_
#define _SQLITE3_H_
//#include <stdarg.h>     /* Needed for the definition of va_list */

/*
** Make sure we can call this stuff from C++.
*/
#if 0
extern "C" {
#endif


/*
** Add the ability to override 'extern'
*/
#ifndef SQLITE_EXTERN
# define SQLITE_EXTERN extern
#endif

#ifndef SQLITE_API
# define SQLITE_API
#endif


/*
** These no-op macros are used in front of interfaces to mark those
** interfaces as either deprecated or experimental.  New applications
** should not use deprecated interfaces - they are support for backwards
** compatibility only.  Application writers should be aware that
** experimental interfaces are subject to change in point releases.
**
** These macros used to resolve to various kinds of compiler magic that
** would generate warning messages when they were used.  But that
** compiler magic ended up generating such a flurry of bug reports
** that we have taken it all out and gone back to using simple
** noop macros.
*/
#define SQLITE_DEPRECATED
#define SQLITE_EXPERIMENTAL

/*
** Ensure these symbols were not defined by some previous header file.
*/
#ifdef SQLITE_VERSION
# undef SQLITE_VERSION
#endif
#ifdef SQLITE_VERSION_NUMBER
# undef SQLITE_VERSION_NUMBER
#endif

/*
** CAPI3REF: Compile-Time Library Version Numbers
**
** ^(The [SQLITE_VERSION] C preprocessor macro in the sqlite3.h header
** evaluates to a string literal that is the SQLite version in the
** format "X.Y.Z" where X is the major version number (always 3 for
** SQLite3) and Y is the minor version number and Z is the release number.)^
** ^(The [SQLITE_VERSION_NUMBER] C preprocessor macro resolves to an integer
** with the value (X*1000000 + Y*1000 + Z) where X, Y, and Z are the same
** numbers used in [SQLITE_VERSION].)^
** The SQLITE_VERSION_NUMBER for any given release of SQLite will also
** be larger than the release from which it is derived.  Either Y will
** be held constant and Z will be incremented or else Y will be incremented
** and Z will be reset to zero.
**
** Since version 3.6.18, SQLite source code has been stored in the
** <a href="http://www.fossil-scm.org/">Fossil configuration management
** system</a>.  ^The SQLITE_SOURCE_ID macro evaluates to
** a string which identifies a particular check-in of SQLite
** within its configuration management system.  ^The SQLITE_SOURCE_ID
** string contains the date and time of the check-in (UTC) and an SHA1
** hash of the entire source tree.
**
** See also: [sqlite3_libversion()],
** [sqlite3_libversion_number()], [sqlite3_sourceid()],
** [sqlite_version()] and [sqlite_source_id()].
*/
#define SQLITE_VERSION        "3.7.6.3"
#define SQLITE_VERSION_NUMBER 3007006
#define SQLITE_SOURCE_ID      "2011-05-19 13:26:54 ed1da510a239ea767a01dc332b667119fa3c908e"

/*
** CAPI3REF: Run-Time Library Version Numbers
** KEYWORDS: sqlite3_version, sqlite3_sourceid
**
** These interfaces provide the same information as the [SQLITE_VERSION],
** [SQLITE_VERSION_NUMBER], and [SQLITE_SOURCE_ID] C preprocessor macros
** but are associated with the library instead of the header file.  ^(Cautious
** programmers might include assert() statements in their application to
** verify that values returned by these interfaces match the macros in
** the header, and thus insure that the application is
** compiled with matching library and header files.
**
** <blockquote><pre>
** assert( sqlite3_libversion_number()==SQLITE_VERSION_NUMBER );
** assert( strcmp(sqlite3_sourceid(),SQLITE_SOURCE_ID)==0 );
** assert( strcmp(sqlite3_libversion(),SQLITE_VERSION)==0 );
** </pre></blockquote>)^
**
** ^The sqlite3_version[] string constant contains the text of [SQLITE_VERSION]
** macro.  ^The sqlite3_libversion() function returns a pointer to the
** to the sqlite3_version[] string constant.  The sqlite3_libversion()
** function is provided for use in DLLs since DLL users usually do not have
** direct access to string constants within the DLL.  ^The
** sqlite3_libversion_number() function returns an integer equal to
** [SQLITE_VERSION_NUMBER].  ^The sqlite3_sourceid() function returns 
** a pointer to a string constant whose value is the same as the 
** [SQLITE_SOURCE_ID] C preprocessor macro.
**
** See also: [sqlite_version()] and [sqlite_source_id()].
*/
#ifndef NODEFINE_SQLITE3_VERSION
SQLITE_API extern const char sqlite3_version[] = SQLITE_VERSION;
#endif
SQLITE_API const char *sqlite3_libversion(void);
SQLITE_API const char *sqlite3_sourceid(void);
SQLITE_API int sqlite3_libversion_number(void);

/*
** CAPI3REF: Run-Time Library Compilation Options Diagnostics
**
** ^The sqlite3_compileoption_used() function returns 0 or 1 
** indicating whether the specified option was defined at 
** compile time.  ^The SQLITE_ prefix may be omitted from the 
** option name passed to sqlite3_compileoption_used().  
**
** ^The sqlite3_compileoption_get() function allows iterating
** over the list of options that were defined at compile time by
** returning the N-th compile time option string.  ^If N is out of range,
** sqlite3_compileoption_get() returns a NULL pointer.  ^The SQLITE_ 
** prefix is omitted from any strings returned by 
** sqlite3_compileoption_get().
**
** ^Support for the diagnostic functions sqlite3_compileoption_used()
** and sqlite3_compileoption_get() may be omitted by specifying the 
** [SQLITE_OMIT_COMPILEOPTION_DIAGS] option at compile time.
**
** See also: SQL functions [sqlite_compileoption_used()] and
** [sqlite_compileoption_get()] and the [compile_options pragma].
*/
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
SQLITE_API int sqlite3_compileoption_used(const char *zOptName);
SQLITE_API const char *sqlite3_compileoption_get(int N);
#endif

/*
** CAPI3REF: Test To See If The Library Is Threadsafe
**
** ^The sqlite3_threadsafe() function returns zero if and only if
** SQLite was compiled mutexing code omitted due to the
** [SQLITE_THREADSAFE] compile-time option being set to 0.
**
** SQLite can be compiled with or without mutexes.  When
** the [SQLITE_THREADSAFE] C preprocessor macro is 1 or 2, mutexes
** are enabled and SQLite is threadsafe.  When the
** [SQLITE_THREADSAFE] macro is 0, 
** the mutexes are omitted.  Without the mutexes, it is not safe
** to use SQLite concurrently from more than one thread.
**
** Enabling mutexes incurs a measurable performance penalty.
** So if speed is of utmost importance, it makes sense to disable
** the mutexes.  But for maximum safety, mutexes should be enabled.
** ^The default behavior is for mutexes to be enabled.
**
** This interface can be used by an application to make sure that the
** version of SQLite that it is linking against was compiled with
** the desired setting of the [SQLITE_THREADSAFE] macro.
**
** This interface only reports on the compile-time mutex setting
** of the [SQLITE_THREADSAFE] flag.  If SQLite is compiled with
** SQLITE_THREADSAFE=1 or =2 then mutexes are enabled by default but
** can be fully or partially disabled using a call to [sqlite3_config()]
** with the verbs [SQLITE_CONFIG_SINGLETHREAD], [SQLITE_CONFIG_MULTITHREAD],
** or [SQLITE_CONFIG_MUTEX].  ^(The return value of the
** sqlite3_threadsafe() function shows only the compile-time setting of
** thread safety, not any run-time changes to that setting made by
** sqlite3_config(). In other words, the return value from sqlite3_threadsafe()
** is unchanged by calls to sqlite3_config().)^
**
** See the [threading mode] documentation for additional information.
*/
SQLITE_API int sqlite3_threadsafe(void);

/*
** CAPI3REF: Database Connection Handle
** KEYWORDS: {database connection} {database connections}
**
** Each open SQLite database is represented by a pointer to an instance of
** the opaque structure named "sqlite3".  It is useful to think of an sqlite3
** pointer as an object.  The [sqlite3_open()], [sqlite3_open16()], and
** [sqlite3_open_v2()] interfaces are its constructors, and [sqlite3_close()]
** is its destructor.  There are many other interfaces (such as
** [sqlite3_prepare_v2()], [sqlite3_create_function()], and
** [sqlite3_busy_timeout()] to name but three) that are methods on an
** sqlite3 object.
*/
typedef struct sqlite3 sqlite3;

/*
** CAPI3REF: 64-Bit Integer Types
** KEYWORDS: sqlite_int64 sqlite_uint64
**
** Because there is no cross-platform way to specify 64-bit integer types
** SQLite includes typedefs for 64-bit signed and unsigned integers.
**
** The sqlite3_int64 and sqlite3_uint64 are the preferred type definitions.
** The sqlite_int64 and sqlite_uint64 types are supported for backwards
** compatibility only.
**
** ^The sqlite3_int64 and sqlite_int64 types can store integer values
** between -9223372036854775808 and +9223372036854775807 inclusive.  ^The
** sqlite3_uint64 and sqlite_uint64 types can store integer values 
** between 0 and +18446744073709551615 inclusive.
*/

typedef int64_t sqlite_int64;
typedef uint64_t sqlite_uint64;
typedef sqlite_int64 sqlite3_int64;
typedef sqlite_uint64 sqlite3_uint64;
  
/*
** If compiling for a processor that lacks floating point support,
** substitute integer for floating-point.
*/
#ifdef SQLITE_OMIT_FLOATING_POINT
# define double sqlite3_int64
#endif

/*
** CAPI3REF: Closing A Database Connection
**
** ^The sqlite3_close() routine is the destructor for the [sqlite3] object.
** ^Calls to sqlite3_close() return SQLITE_OK if the [sqlite3] object is
** successfully destroyed and all associated resources are deallocated.
**
** Applications must [sqlite3_finalize | finalize] all [prepared statements]
** and [sqlite3_blob_close | close] all [BLOB handles] associated with
** the [sqlite3] object prior to attempting to close the object.  ^If
** sqlite3_close() is called on a [database connection] that still has
** outstanding [prepared statements] or [BLOB handles], then it returns
** SQLITE_BUSY.
**
** ^If [sqlite3_close()] is invoked while a transaction is open,
** the transaction is automatically rolled back.
**
** The C parameter to [sqlite3_close(C)] must be either a NULL
** pointer or an [sqlite3] object pointer obtained
** from [sqlite3_open()], [sqlite3_open16()], or
** [sqlite3_open_v2()], and not previously closed.
** ^Calling sqlite3_close() with a NULL pointer argument is a 
** harmless no-op.
*/
SQLITE_API int sqlite3_close(sqlite3 *);

/*
** The type for a callback function.
** This is legacy and deprecated.  It is included for historical
** compatibility and is not documented.
*/
typedef int (*sqlite3_callback)(void*,int,char**, char**);

/*
** CAPI3REF: One-Step Query Execution Interface
**
** The sqlite3_exec() interface is a convenience wrapper around
** [sqlite3_prepare_v2()], [sqlite3_step()], and [sqlite3_finalize()],
** that allows an application to run multiple statements of SQL
** without having to use a lot of C code. 
**
** ^The sqlite3_exec() interface runs zero or more UTF-8 encoded,
** semicolon-separate SQL statements passed into its 2nd argument,
** in the context of the [database connection] passed in as its 1st
** argument.  ^If the callback function of the 3rd argument to
** sqlite3_exec() is not NULL, then it is invoked for each result row
** coming out of the evaluated SQL statements.  ^The 4th argument to
** to sqlite3_exec() is relayed through to the 1st argument of each
** callback invocation.  ^If the callback pointer to sqlite3_exec()
** is NULL, then no callback is ever invoked and result rows are
** ignored.
**
** ^If an error occurs while evaluating the SQL statements passed into
** sqlite3_exec(), then execution of the current statement stops and
** subsequent statements are skipped.  ^If the 5th parameter to sqlite3_exec()
** is not NULL then any error message is written into memory obtained
** from [sqlite3_malloc()] and passed back through the 5th parameter.
** To avoid memory leaks, the application should invoke [sqlite3_free()]
** on error message strings returned through the 5th parameter of
** of sqlite3_exec() after the error message string is no longer needed.
** ^If the 5th parameter to sqlite3_exec() is not NULL and no errors
** occur, then sqlite3_exec() sets the pointer in its 5th parameter to
** NULL before returning.
**
** ^If an sqlite3_exec() callback returns non-zero, the sqlite3_exec()
** routine returns SQLITE_ABORT without invoking the callback again and
** without running any subsequent SQL statements.
**
** ^The 2nd argument to the sqlite3_exec() callback function is the
** number of columns in the result.  ^The 3rd argument to the sqlite3_exec()
** callback is an array of pointers to strings obtained as if from
** [sqlite3_column_text()], one for each column.  ^If an element of a
** result row is NULL then the corresponding string pointer for the
** sqlite3_exec() callback is a NULL pointer.  ^The 4th argument to the
** sqlite3_exec() callback is an array of pointers to strings where each
** entry represents the name of corresponding result column as obtained
** from [sqlite3_column_name()].
**
** ^If the 2nd parameter to sqlite3_exec() is a NULL pointer, a pointer
** to an empty string, or a pointer that contains only whitespace and/or 
** SQL comments, then no SQL statements are evaluated and the database
** is not changed.
**
** Restrictions:
**
** <ul>
** <li> The application must insure that the 1st parameter to sqlite3_exec()
**      is a valid and open [database connection].
** <li> The application must not close [database connection] specified by
**      the 1st parameter to sqlite3_exec() while sqlite3_exec() is running.
** <li> The application must not modify the SQL statement text passed into
**      the 2nd parameter of sqlite3_exec() while sqlite3_exec() is running.
** </ul>
*/
SQLITE_API int sqlite3_exec(
  sqlite3*,                                  /* An open database */
  const char *sql,                           /* SQL to be evaluated */
  int (*callback)(void*,int,char**,char**),  /* Callback function */
  void *,                                    /* 1st argument to callback */
  char **errmsg                              /* Error msg written here */
);

/*
** CAPI3REF: Result Codes
** KEYWORDS: SQLITE_OK {error code} {error codes}
** KEYWORDS: {result code} {result codes}
**
** Many SQLite functions return an integer result code from the set shown
** here in order to indicates success or failure.
**
** New error codes may be added in future versions of SQLite.
**
** See also: [SQLITE_IOERR_READ | extended result codes]
*/
#define SQLITE_OK           0   /* Successful result */
/* beginning-of-error-codes */
#define SQLITE_ERROR        1   /* SQL error or missing database */
#define SQLITE_INTERNAL     2   /* Internal logic error in SQLite */
#define SQLITE_PERM         3   /* Access permission denied */
#define SQLITE_ABORT        4   /* Callback routine requested an abort */
#define SQLITE_BUSY         5   /* The database file is locked */
#define SQLITE_LOCKED       6   /* A table in the database is locked */
#define SQLITE_NOMEM        7   /* A malloc() failed */
#define SQLITE_READONLY     8   /* Attempt to write a readonly database */
#define SQLITE_INTERRUPT    9   /* Operation terminated by sqlite3_interrupt()*/
#define SQLITE_IOERR       10   /* Some kind of disk I/O error occurred */
#define SQLITE_CORRUPT     11   /* The database disk image is malformed */
#define SQLITE_NOTFOUND    12   /* Unknown opcode in sqlite3_file_control() */
#define SQLITE_FULL        13   /* Insertion failed because database is full */
#define SQLITE_CANTOPEN    14   /* Unable to open the database file */
#define SQLITE_PROTOCOL    15   /* Database lock protocol error */
#define SQLITE_EMPTY       16   /* Database is empty */
#define SQLITE_SCHEMA      17   /* The database schema changed */
#define SQLITE_TOOBIG      18   /* String or BLOB exceeds size limit */
#define SQLITE_CONSTRAINT  19   /* Abort due to constraint violation */
#define SQLITE_MISMATCH    20   /* Data type mismatch */
#define SQLITE_MISUSE      21   /* Library used incorrectly */
#define SQLITE_NOLFS       22   /* Uses OS features not supported on host */
#define SQLITE_AUTH        23   /* Authorization denied */
#define SQLITE_FORMAT      24   /* Auxiliary database format error */
#define SQLITE_RANGE       25   /* 2nd parameter to sqlite3_bind out of range */
#define SQLITE_NOTADB      26   /* File opened that is not a database file */
#define SQLITE_ROW         100  /* sqlite3_step() has another row ready */
#define SQLITE_DONE        101  /* sqlite3_step() has finished executing */
/* end-of-error-codes */

/*
** CAPI3REF: Extended Result Codes
** KEYWORDS: {extended error code} {extended error codes}
** KEYWORDS: {extended result code} {extended result codes}
**
** In its default configuration, SQLite API routines return one of 26 integer
** [SQLITE_OK | result codes].  However, experience has shown that many of
** these result codes are too coarse-grained.  They do not provide as
** much information about problems as programmers might like.  In an effort to
** address this, newer versions of SQLite (version 3.3.8 and later) include
** support for additional result codes that provide more detailed information
** about errors. The extended result codes are enabled or disabled
** on a per database connection basis using the
** [sqlite3_extended_result_codes()] API.
**
** Some of the available extended result codes are listed here.
** One may expect the number of extended result codes will be expand
** over time.  Software that uses extended result codes should expect
** to see new result codes in future releases of SQLite.
**
** The SQLITE_OK result code will never be extended.  It will always
** be exactly zero.
*/
#define SQLITE_IOERR_READ              (SQLITE_IOERR | (1<<8))
#define SQLITE_IOERR_SHORT_READ        (SQLITE_IOERR | (2<<8))
#define SQLITE_IOERR_WRITE             (SQLITE_IOERR | (3<<8))
#define SQLITE_IOERR_FSYNC             (SQLITE_IOERR | (4<<8))
#define SQLITE_IOERR_DIR_FSYNC         (SQLITE_IOERR | (5<<8))
#define SQLITE_IOERR_TRUNCATE          (SQLITE_IOERR | (6<<8))
#define SQLITE_IOERR_FSTAT             (SQLITE_IOERR | (7<<8))
#define SQLITE_IOERR_UNLOCK            (SQLITE_IOERR | (8<<8))
#define SQLITE_IOERR_RDLOCK            (SQLITE_IOERR | (9<<8))
#define SQLITE_IOERR_DELETE            (SQLITE_IOERR | (10<<8))
#define SQLITE_IOERR_BLOCKED           (SQLITE_IOERR | (11<<8))
#define SQLITE_IOERR_NOMEM             (SQLITE_IOERR | (12<<8))
#define SQLITE_IOERR_ACCESS            (SQLITE_IOERR | (13<<8))
#define SQLITE_IOERR_CHECKRESERVEDLOCK (SQLITE_IOERR | (14<<8))
#define SQLITE_IOERR_LOCK              (SQLITE_IOERR | (15<<8))
#define SQLITE_IOERR_CLOSE             (SQLITE_IOERR | (16<<8))
#define SQLITE_IOERR_DIR_CLOSE         (SQLITE_IOERR | (17<<8))
#define SQLITE_IOERR_SHMOPEN           (SQLITE_IOERR | (18<<8))
#define SQLITE_IOERR_SHMSIZE           (SQLITE_IOERR | (19<<8))
#define SQLITE_IOERR_SHMLOCK           (SQLITE_IOERR | (20<<8))
#define SQLITE_LOCKED_SHAREDCACHE      (SQLITE_LOCKED |  (1<<8))
#define SQLITE_BUSY_RECOVERY           (SQLITE_BUSY   |  (1<<8))
#define SQLITE_CANTOPEN_NOTEMPDIR      (SQLITE_CANTOPEN | (1<<8))

/*
** CAPI3REF: Flags For File Open Operations
**
** These bit values are intended for use in the
** 3rd parameter to the [sqlite3_open_v2()] interface and
** in the 4th parameter to the xOpen method of the
** [sqlite3_vfs] object.
*/
#define SQLITE_OPEN_READONLY         0x00000001  /* Ok for sqlite3_open_v2() */
#define SQLITE_OPEN_READWRITE        0x00000002  /* Ok for sqlite3_open_v2() */
#define SQLITE_OPEN_CREATE           0x00000004  /* Ok for sqlite3_open_v2() */
#define SQLITE_OPEN_DELETEONCLOSE    0x00000008  /* VFS only */
#define SQLITE_OPEN_EXCLUSIVE        0x00000010  /* VFS only */
#define SQLITE_OPEN_AUTOPROXY        0x00000020  /* VFS only */
#define SQLITE_OPEN_MAIN_DB          0x00000100  /* VFS only */
#define SQLITE_OPEN_TEMP_DB          0x00000200  /* VFS only */
#define SQLITE_OPEN_TRANSIENT_DB     0x00000400  /* VFS only */
#define SQLITE_OPEN_MAIN_JOURNAL     0x00000800  /* VFS only */
#define SQLITE_OPEN_TEMP_JOURNAL     0x00001000  /* VFS only */
#define SQLITE_OPEN_SUBJOURNAL       0x00002000  /* VFS only */
#define SQLITE_OPEN_MASTER_JOURNAL   0x00004000  /* VFS only */
#define SQLITE_OPEN_NOMUTEX          0x00008000  /* Ok for sqlite3_open_v2() */
#define SQLITE_OPEN_FULLMUTEX        0x00010000  /* Ok for sqlite3_open_v2() */
#define SQLITE_OPEN_SHAREDCACHE      0x00020000  /* Ok for sqlite3_open_v2() */
#define SQLITE_OPEN_PRIVATECACHE     0x00040000  /* Ok for sqlite3_open_v2() */
#define SQLITE_OPEN_WAL              0x00080000  /* VFS only */

/* Reserved:                         0x00F00000 */

/*
** CAPI3REF: Device Characteristics
**
** The xDeviceCharacteristics method of the [sqlite3_io_methods]
** object returns an integer which is a vector of the these
** bit values expressing I/O characteristics of the mass storage
** device that holds the file that the [sqlite3_io_methods]
** refers to.
**
** The SQLITE_IOCAP_ATOMIC property means that all writes of
** any size are atomic.  The SQLITE_IOCAP_ATOMICnnn values
** mean that writes of blocks that are nnn bytes in size and
** are aligned to an address which is an integer multiple of
** nnn are atomic.  The SQLITE_IOCAP_SAFE_APPEND value means
** that when data is appended to a file, the data is appended
** first then the size of the file is extended, never the other
** way around.  The SQLITE_IOCAP_SEQUENTIAL property means that
** information is written to disk in the same order as calls
** to xWrite().
*/
#define SQLITE_IOCAP_ATOMIC                 0x00000001
#define SQLITE_IOCAP_ATOMIC512              0x00000002
#define SQLITE_IOCAP_ATOMIC1K               0x00000004
#define SQLITE_IOCAP_ATOMIC2K               0x00000008
#define SQLITE_IOCAP_ATOMIC4K               0x00000010
#define SQLITE_IOCAP_ATOMIC8K               0x00000020
#define SQLITE_IOCAP_ATOMIC16K              0x00000040
#define SQLITE_IOCAP_ATOMIC32K              0x00000080
#define SQLITE_IOCAP_ATOMIC64K              0x00000100
#define SQLITE_IOCAP_SAFE_APPEND            0x00000200
#define SQLITE_IOCAP_SEQUENTIAL             0x00000400
#define SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN  0x00000800

/*
** CAPI3REF: File Locking Levels
**
** SQLite uses one of these integer values as the second
** argument to calls it makes to the xLock() and xUnlock() methods
** of an [sqlite3_io_methods] object.
*/
#define SQLITE_LOCK_NONE          0
#define SQLITE_LOCK_SHARED        1
#define SQLITE_LOCK_RESERVED      2
#define SQLITE_LOCK_PENDING       3
#define SQLITE_LOCK_EXCLUSIVE     4

/*
** CAPI3REF: Synchronization Type Flags
**
** When SQLite invokes the xSync() method of an
** [sqlite3_io_methods] object it uses a combination of
** these integer values as the second argument.
**
** When the SQLITE_SYNC_DATAONLY flag is used, it means that the
** sync operation only needs to flush data to mass storage.  Inode
** information need not be flushed. If the lower four bits of the flag
** equal SQLITE_SYNC_NORMAL, that means to use normal fsync() semantics.
** If the lower four bits equal SQLITE_SYNC_FULL, that means
** to use Mac OS X style fullsync instead of fsync().
**
** Do not confuse the SQLITE_SYNC_NORMAL and SQLITE_SYNC_FULL flags
** with the [PRAGMA synchronous]=NORMAL and [PRAGMA synchronous]=FULL
** settings.  The [synchronous pragma] determines when calls to the
** xSync VFS method occur and applies uniformly across all platforms.
** The SQLITE_SYNC_NORMAL and SQLITE_SYNC_FULL flags determine how
** energetic or rigorous or forceful the sync operations are and
** only make a difference on Mac OSX for the default SQLite code.
** (Third-party VFS implementations might also make the distinction
** between SQLITE_SYNC_NORMAL and SQLITE_SYNC_FULL, but among the
** operating systems natively supported by SQLite, only Mac OSX
** cares about the difference.)
*/
#define SQLITE_SYNC_NORMAL        0x00002
#define SQLITE_SYNC_FULL          0x00003
#define SQLITE_SYNC_DATAONLY      0x00010

/*
** CAPI3REF: OS Interface Open File Handle
**
** An [sqlite3_file] object represents an open file in the 
** [sqlite3_vfs | OS interface layer].  Individual OS interface
** implementations will
** want to subclass this object by appending additional fields
** for their own use.  The pMethods entry is a pointer to an
** [sqlite3_io_methods] object that defines methods for performing
** I/O operations on the open file.
*/
typedef struct sqlite3_file sqlite3_file;
struct sqlite3_file {
  const struct sqlite3_io_methods *pMethods;  /* Methods for an open file */
};

/*
** CAPI3REF: OS Interface File Virtual Methods Object
**
** Every file opened by the [sqlite3_vfs] xOpen method populates an
** [sqlite3_file] object (or, more commonly, a subclass of the
** [sqlite3_file] object) with a pointer to an instance of this object.
** This object defines the methods used to perform various operations
** against the open file represented by the [sqlite3_file] object.
**
** If the xOpen method sets the sqlite3_file.pMethods element 
** to a non-NULL pointer, then the sqlite3_io_methods.xClose method
** may be invoked even if the xOpen reported that it failed.  The
** only way to prevent a call to xClose following a failed xOpen
** is for the xOpen to set the sqlite3_file.pMethods element to NULL.
**
** The flags argument to xSync may be one of [SQLITE_SYNC_NORMAL] or
** [SQLITE_SYNC_FULL].  The first choice is the normal fsync().
** The second choice is a Mac OS X style fullsync.  The [SQLITE_SYNC_DATAONLY]
** flag may be ORed in to indicate that only the data of the file
** and not its inode needs to be synced.
**
** The integer values to xLock() and xUnlock() are one of
** <ul>
** <li> [SQLITE_LOCK_NONE],
** <li> [SQLITE_LOCK_SHARED],
** <li> [SQLITE_LOCK_RESERVED],
** <li> [SQLITE_LOCK_PENDING], or
** <li> [SQLITE_LOCK_EXCLUSIVE].
** </ul>
** xLock() increases the lock. xUnlock() decreases the lock.
** The xCheckReservedLock() method checks whether any database connection,
** either in this process or in some other process, is holding a RESERVED,
** PENDING, or EXCLUSIVE lock on the file.  It returns true
** if such a lock exists and false otherwise.
**
** The xFileControl() method is a generic interface that allows custom
** VFS implementations to directly control an open file using the
** [sqlite3_file_control()] interface.  The second "op" argument is an
** integer opcode.  The third argument is a generic pointer intended to
** point to a structure that may contain arguments or space in which to
** write return values.  Potential uses for xFileControl() might be
** functions to enable blocking locks with timeouts, to change the
** locking strategy (for example to use dot-file locks), to inquire
** about the status of a lock, or to break stale locks.  The SQLite
** core reserves all opcodes less than 100 for its own use.
** A [SQLITE_FCNTL_LOCKSTATE | list of opcodes] less than 100 is available.
** Applications that define a custom xFileControl method should use opcodes
** greater than 100 to avoid conflicts.  VFS implementations should
** return [SQLITE_NOTFOUND] for file control opcodes that they do not
** recognize.
**
** The xSectorSize() method returns the sector size of the
** device that underlies the file.  The sector size is the
** minimum write that can be performed without disturbing
** other bytes in the file.  The xDeviceCharacteristics()
** method returns a bit vector describing behaviors of the
** underlying device:
**
** <ul>
** <li> [SQLITE_IOCAP_ATOMIC]
** <li> [SQLITE_IOCAP_ATOMIC512]
** <li> [SQLITE_IOCAP_ATOMIC1K]
** <li> [SQLITE_IOCAP_ATOMIC2K]
** <li> [SQLITE_IOCAP_ATOMIC4K]
** <li> [SQLITE_IOCAP_ATOMIC8K]
** <li> [SQLITE_IOCAP_ATOMIC16K]
** <li> [SQLITE_IOCAP_ATOMIC32K]
** <li> [SQLITE_IOCAP_ATOMIC64K]
** <li> [SQLITE_IOCAP_SAFE_APPEND]
** <li> [SQLITE_IOCAP_SEQUENTIAL]
** </ul>
**
** The SQLITE_IOCAP_ATOMIC property means that all writes of
** any size are atomic.  The SQLITE_IOCAP_ATOMICnnn values
** mean that writes of blocks that are nnn bytes in size and
** are aligned to an address which is an integer multiple of
** nnn are atomic.  The SQLITE_IOCAP_SAFE_APPEND value means
** that when data is appended to a file, the data is appended
** first then the size of the file is extended, never the other
** way around.  The SQLITE_IOCAP_SEQUENTIAL property means that
** information is written to disk in the same order as calls
** to xWrite().
**
** If xRead() returns SQLITE_IOERR_SHORT_READ it must also fill
** in the unread portions of the buffer with zeros.  A VFS that
** fails to zero-fill short reads might seem to work.  However,
** failure to zero-fill short reads will eventually lead to
** database corruption.
*/
typedef struct sqlite3_io_methods sqlite3_io_methods;
struct sqlite3_io_methods {
  int iVersion;
  int (*xClose)(sqlite3_file*);
  int (*xRead)(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
  int (*xWrite)(sqlite3_file*, const void*, int iAmt, sqlite3_int64 iOfst);
  int (*xTruncate)(sqlite3_file*, sqlite3_int64 size);
  int (*xSync)(sqlite3_file*, int flags);
  int (*xFileSize)(sqlite3_file*, sqlite3_int64 *pSize);
  int (*xLock)(sqlite3_file*, int);
  int (*xUnlock)(sqlite3_file*, int);
  int (*xCheckReservedLock)(sqlite3_file*, int *pResOut);
  int (*xFileControl)(sqlite3_file*, int op, void *pArg);
  int (*xSectorSize)(sqlite3_file*);
  int (*xDeviceCharacteristics)(sqlite3_file*);
  /* Methods above are valid for version 1 */
  int (*xShmMap)(sqlite3_file*, int iPg, int pgsz, int, void volatile**);
  int (*xShmLock)(sqlite3_file*, int offset, int n, int flags);
  void (*xShmBarrier)(sqlite3_file*);
  int (*xShmUnmap)(sqlite3_file*, int deleteFlag);
  /* Methods above are valid for version 2 */
  /* Additional methods may be added in future releases */
};

/*
** CAPI3REF: Standard File Control Opcodes
**
** These integer constants are opcodes for the xFileControl method
** of the [sqlite3_io_methods] object and for the [sqlite3_file_control()]
** interface.
**
** The [SQLITE_FCNTL_LOCKSTATE] opcode is used for debugging.  This
** opcode causes the xFileControl method to write the current state of
** the lock (one of [SQLITE_LOCK_NONE], [SQLITE_LOCK_SHARED],
** [SQLITE_LOCK_RESERVED], [SQLITE_LOCK_PENDING], or [SQLITE_LOCK_EXCLUSIVE])
** into an integer that the pArg argument points to. This capability
** is used during testing and only needs to be supported when SQLITE_TEST
** is defined.
**
** The [SQLITE_FCNTL_SIZE_HINT] opcode is used by SQLite to give the VFS
** layer a hint of how large the database file will grow to be during the
** current transaction.  This hint is not guaranteed to be accurate but it
** is often close.  The underlying VFS might choose to preallocate database
** file space based on this hint in order to help writes to the database
** file run faster.
**
** The [SQLITE_FCNTL_CHUNK_SIZE] opcode is used to request that the VFS
** extends and truncates the database file in chunks of a size specified
** by the user. The fourth argument to [sqlite3_file_control()] should 
** point to an integer (type int) containing the new chunk-size to use
** for the nominated database. Allocating database file space in large
** chunks (say 1MB at a time), may reduce file-system fragmentation and
** improve performance on some systems.
**
** The [SQLITE_FCNTL_FILE_POINTER] opcode is used to obtain a pointer
** to the [sqlite3_file] object associated with a particular database
** connection.  See the [sqlite3_file_control()] documentation for
** additional information.
**
** ^(The [SQLITE_FCNTL_SYNC_OMITTED] opcode is generated internally by
** SQLite and sent to all VFSes in place of a call to the xSync method
** when the database connection has [PRAGMA synchronous] set to OFF.)^
** Some specialized VFSes need this signal in order to operate correctly
** when [PRAGMA synchronous | PRAGMA synchronous=OFF] is set, but most 
** VFSes do not need this signal and should silently ignore this opcode.
** Applications should not call [sqlite3_file_control()] with this
** opcode as doing so may disrupt the operation of the specialized VFSes
** that do require it.  
*/
#define SQLITE_FCNTL_LOCKSTATE        1
#define SQLITE_GET_LOCKPROXYFILE      2
#define SQLITE_SET_LOCKPROXYFILE      3
#define SQLITE_LAST_ERRNO             4
#define SQLITE_FCNTL_SIZE_HINT        5
#define SQLITE_FCNTL_CHUNK_SIZE       6
#define SQLITE_FCNTL_FILE_POINTER     7
#define SQLITE_FCNTL_SYNC_OMITTED     8


/*
** CAPI3REF: Mutex Handle
**
** The mutex module within SQLite defines [sqlite3_mutex] to be an
** abstract type for a mutex object.  The SQLite core never looks
** at the internal representation of an [sqlite3_mutex].  It only
** deals with pointers to the [sqlite3_mutex] object.
**
** Mutexes are created using [sqlite3_mutex_alloc()].
*/
typedef struct sqlite3_mutex sqlite3_mutex;

/*
** CAPI3REF: OS Interface Object
**
** An instance of the sqlite3_vfs object defines the interface between
** the SQLite core and the underlying operating system.  The "vfs"
** in the name of the object stands for "virtual file system".
**
** The value of the iVersion field is initially 1 but may be larger in
** future versions of SQLite.  Additional fields may be appended to this
** object when the iVersion value is increased.  Note that the structure
** of the sqlite3_vfs object changes in the transaction between
** SQLite version 3.5.9 and 3.6.0 and yet the iVersion field was not
** modified.
**
** The szOsFile field is the size of the subclassed [sqlite3_file]
** structure used by this VFS.  mxPathname is the maximum length of
** a pathname in this VFS.
**
** Registered sqlite3_vfs objects are kept on a linked list formed by
** the pNext pointer.  The [sqlite3_vfs_register()]
** and [sqlite3_vfs_unregister()] interfaces manage this list
** in a thread-safe way.  The [sqlite3_vfs_find()] interface
** searches the list.  Neither the application code nor the VFS
** implementation should use the pNext pointer.
**
** The pNext field is the only field in the sqlite3_vfs
** structure that SQLite will ever modify.  SQLite will only access
** or modify this field while holding a particular static mutex.
** The application should never modify anything within the sqlite3_vfs
** object once the object has been registered.
**
** The zName field holds the name of the VFS module.  The name must
** be unique across all VFS modules.
**
** ^SQLite guarantees that the zFilename parameter to xOpen
** is either a NULL pointer or string obtained
** from xFullPathname() with an optional suffix added.
** ^If a suffix is added to the zFilename parameter, it will
** consist of a single "-" character followed by no more than
** 10 alphanumeric and/or "-" characters.
** ^SQLite further guarantees that
** the string will be valid and unchanged until xClose() is
** called. Because of the previous sentence,
** the [sqlite3_file] can safely store a pointer to the
** filename if it needs to remember the filename for some reason.
** If the zFilename parameter to xOpen is a NULL pointer then xOpen
** must invent its own temporary name for the file.  ^Whenever the 
** xFilename parameter is NULL it will also be the case that the
** flags parameter will include [SQLITE_OPEN_DELETEONCLOSE].
**
** The flags argument to xOpen() includes all bits set in
** the flags argument to [sqlite3_open_v2()].  Or if [sqlite3_open()]
** or [sqlite3_open16()] is used, then flags includes at least
** [SQLITE_OPEN_READWRITE] | [SQLITE_OPEN_CREATE]. 
** If xOpen() opens a file read-only then it sets *pOutFlags to
** include [SQLITE_OPEN_READONLY].  Other bits in *pOutFlags may be set.
**
** ^(SQLite will also add one of the following flags to the xOpen()
** call, depending on the object being opened:
**
** <ul>
** <li>  [SQLITE_OPEN_MAIN_DB]
** <li>  [SQLITE_OPEN_MAIN_JOURNAL]
** <li>  [SQLITE_OPEN_TEMP_DB]
** <li>  [SQLITE_OPEN_TEMP_JOURNAL]
** <li>  [SQLITE_OPEN_TRANSIENT_DB]
** <li>  [SQLITE_OPEN_SUBJOURNAL]
** <li>  [SQLITE_OPEN_MASTER_JOURNAL]
** <li>  [SQLITE_OPEN_WAL]
** </ul>)^
**
** The file I/O implementation can use the object type flags to
** change the way it deals with files.  For example, an application
** that does not care about crash recovery or rollback might make
** the open of a journal file a no-op.  Writes to this journal would
** also be no-ops, and any attempt to read the journal would return
** SQLITE_IOERR.  Or the implementation might recognize that a database
** file will be doing page-aligned sector reads and writes in a random
** order and set up its I/O subsystem accordingly.
**
** SQLite might also add one of the following flags to the xOpen method:
**
** <ul>
** <li> [SQLITE_OPEN_DELETEONCLOSE]
** <li> [SQLITE_OPEN_EXCLUSIVE]
** </ul>
**
** The [SQLITE_OPEN_DELETEONCLOSE] flag means the file should be
** deleted when it is closed.  ^The [SQLITE_OPEN_DELETEONCLOSE]
** will be set for TEMP databases and their journals, transient
** databases, and subjournals.
**
** ^The [SQLITE_OPEN_EXCLUSIVE] flag is always used in conjunction
** with the [SQLITE_OPEN_CREATE] flag, which are both directly
** analogous to the O_EXCL and O_CREAT flags of the POSIX open()
** API.  The SQLITE_OPEN_EXCLUSIVE flag, when paired with the 
** SQLITE_OPEN_CREATE, is used to indicate that file should always
** be created, and that it is an error if it already exists.
** It is <i>not</i> used to indicate the file should be opened 
** for exclusive access.
**
** ^At least szOsFile bytes of memory are allocated by SQLite
** to hold the  [sqlite3_file] structure passed as the third
** argument to xOpen.  The xOpen method does not have to
** allocate the structure; it should just fill it in.  Note that
** the xOpen method must set the sqlite3_file.pMethods to either
** a valid [sqlite3_io_methods] object or to NULL.  xOpen must do
** this even if the open fails.  SQLite expects that the sqlite3_file.pMethods
** element will be valid after xOpen returns regardless of the success
** or failure of the xOpen call.
**
** ^The flags argument to xAccess() may be [SQLITE_ACCESS_EXISTS]
** to test for the existence of a file, or [SQLITE_ACCESS_READWRITE] to
** test whether a file is readable and writable, or [SQLITE_ACCESS_READ]
** to test whether a file is at least readable.   The file can be a
** directory.
**
** ^SQLite will always allocate at least mxPathname+1 bytes for the
** output buffer xFullPathname.  The exact size of the output buffer
** is also passed as a parameter to both  methods. If the output buffer
** is not large enough, [SQLITE_CANTOPEN] should be returned. Since this is
** handled as a fatal error by SQLite, vfs implementations should endeavor
** to prevent this by setting mxPathname to a sufficiently large value.
**
** The xRandomness(), xSleep(), xCurrentTime(), and xCurrentTimeInt64()
** interfaces are not strictly a part of the filesystem, but they are
** included in the VFS structure for completeness.
** The xRandomness() function attempts to return nBytes bytes
** of good-quality randomness into zOut.  The return value is
** the actual number of bytes of randomness obtained.
** The xSleep() method causes the calling thread to sleep for at
** least the number of microseconds given.  ^The xCurrentTime()
** method returns a Julian Day Number for the current date and time as
** a floating point value.
** ^The xCurrentTimeInt64() method returns, as an integer, the Julian
** Day Number multipled by 86400000 (the number of milliseconds in 
** a 24-hour day).  
** ^SQLite will use the xCurrentTimeInt64() method to get the current
** date and time if that method is available (if iVersion is 2 or 
** greater and the function pointer is not NULL) and will fall back
** to xCurrentTime() if xCurrentTimeInt64() is unavailable.
**
** ^The xSetSystemCall(), xGetSystemCall(), and xNestSystemCall() interfaces
** are not used by the SQLite core.  These optional interfaces are provided
** by some VFSes to facilitate testing of the VFS code. By overriding 
** system calls with functions under its control, a test program can
** simulate faults and error conditions that would otherwise be difficult
** or impossible to induce.  The set of system calls that can be overridden
** varies from one VFS to another, and from one version of the same VFS to the
** next.  Applications that use these interfaces must be prepared for any
** or all of these interfaces to be NULL or for their behavior to change
** from one release to the next.  Applications must not attempt to access
** any of these methods if the iVersion of the VFS is less than 3.
*/
typedef struct sqlite3_vfs sqlite3_vfs;
typedef void (*sqlite3_syscall_ptr)(void);
struct sqlite3_vfs {
  int iVersion;            /* Structure version number (currently 3) */
  int szOsFile;            /* Size of subclassed sqlite3_file */
  int mxPathname;          /* Maximum file pathname length */
  sqlite3_vfs *pNext;      /* Next registered VFS */
  const char *zName;       /* Name of this virtual file system */
  void *pAppData;          /* Pointer to application-specific data */
  int (*xOpen)(sqlite3_vfs*, const char *zName, sqlite3_file*,
               int flags, int *pOutFlags);
  int (*xDelete)(sqlite3_vfs*, const char *zName, int syncDir);
  int (*xAccess)(sqlite3_vfs*, const char *zName, int flags, int *pResOut);
  int (*xFullPathname)(sqlite3_vfs*, const char *zName, int nOut, char *zOut);
  void *(*xDlOpen)(sqlite3_vfs*, const char *zFilename);
  void (*xDlError)(sqlite3_vfs*, int nByte, char *zErrMsg);
  void (*(*xDlSym)(sqlite3_vfs*,void*, const char *zSymbol))(void);
  void (*xDlClose)(sqlite3_vfs*, void*);
  int (*xRandomness)(sqlite3_vfs*, int nByte, char *zOut);
  int (*xSleep)(sqlite3_vfs*, int microseconds);
  int (*xCurrentTime)(sqlite3_vfs*, double*);
  int (*xGetLastError)(sqlite3_vfs*, int, char *);
  /*
  ** The methods above are in version 1 of the sqlite_vfs object
  ** definition.  Those that follow are added in version 2 or later
  */
  int (*xCurrentTimeInt64)(sqlite3_vfs*, sqlite3_int64*);
  /*
  ** The methods above are in versions 1 and 2 of the sqlite_vfs object.
  ** Those below are for version 3 and greater.
  */
  int (*xSetSystemCall)(sqlite3_vfs*, const char *zName, sqlite3_syscall_ptr);
  sqlite3_syscall_ptr (*xGetSystemCall)(sqlite3_vfs*, const char *zName);
  const char *(*xNextSystemCall)(sqlite3_vfs*, const char *zName);
  /*
  ** The methods above are in versions 1 through 3 of the sqlite_vfs object.
  ** New fields may be appended in figure versions.  The iVersion
  ** value will increment whenever this happens. 
  */
};

/*
** CAPI3REF: Flags for the xAccess VFS method
**
** These integer constants can be used as the third parameter to
** the xAccess method of an [sqlite3_vfs] object.  They determine
** what kind of permissions the xAccess method is looking for.
** With SQLITE_ACCESS_EXISTS, the xAccess method
** simply checks whether the file exists.
** With SQLITE_ACCESS_READWRITE, the xAccess method
** checks whether the named directory is both readable and writable
** (in other words, if files can be added, removed, and renamed within
** the directory).
** The SQLITE_ACCESS_READWRITE constant is currently used only by the
** [temp_store_directory pragma], though this could change in a future
** release of SQLite.
** With SQLITE_ACCESS_READ, the xAccess method
** checks whether the file is readable.  The SQLITE_ACCESS_READ constant is
** currently unused, though it might be used in a future release of
** SQLite.
*/
#define SQLITE_ACCESS_EXISTS    0
#define SQLITE_ACCESS_READWRITE 1   /* Used by PRAGMA temp_store_directory */
#define SQLITE_ACCESS_READ      2   /* Unused */

/*
** CAPI3REF: Flags for the xShmLock VFS method
**
** These integer constants define the various locking operations
** allowed by the xShmLock method of [sqlite3_io_methods].  The
** following are the only legal combinations of flags to the
** xShmLock method:
**
** <ul>
** <li>  SQLITE_SHM_LOCK | SQLITE_SHM_SHARED
** <li>  SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE
** <li>  SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED
** <li>  SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE
** </ul>
**
** When unlocking, the same SHARED or EXCLUSIVE flag must be supplied as
** was given no the corresponding lock.  
**
** The xShmLock method can transition between unlocked and SHARED or
** between unlocked and EXCLUSIVE.  It cannot transition between SHARED
** and EXCLUSIVE.
*/
#define SQLITE_SHM_UNLOCK       1
#define SQLITE_SHM_LOCK         2
#define SQLITE_SHM_SHARED       4
#define SQLITE_SHM_EXCLUSIVE    8

/*
** CAPI3REF: Maximum xShmLock index
**
** The xShmLock method on [sqlite3_io_methods] may use values
** between 0 and this upper bound as its "offset" argument.
** The SQLite core will never attempt to acquire or release a
** lock outside of this range
*/
#define SQLITE_SHM_NLOCK        8


/*
** CAPI3REF: Initialize The SQLite Library
**
** ^The sqlite3_initialize() routine initializes the
** SQLite library.  ^The sqlite3_shutdown() routine
** deallocates any resources that were allocated by sqlite3_initialize().
** These routines are designed to aid in process initialization and
** shutdown on embedded systems.  Workstation applications using
** SQLite normally do not need to invoke either of these routines.
**
** A call to sqlite3_initialize() is an "effective" call if it is
** the first time sqlite3_initialize() is invoked during the lifetime of
** the process, or if it is the first time sqlite3_initialize() is invoked
** following a call to sqlite3_shutdown().  ^(Only an effective call
** of sqlite3_initialize() does any initialization.  All other calls
** are harmless no-ops.)^
**
** A call to sqlite3_shutdown() is an "effective" call if it is the first
** call to sqlite3_shutdown() since the last sqlite3_initialize().  ^(Only
** an effective call to sqlite3_shutdown() does any deinitialization.
** All other valid calls to sqlite3_shutdown() are harmless no-ops.)^
**
** The sqlite3_initialize() interface is threadsafe, but sqlite3_shutdown()
** is not.  The sqlite3_shutdown() interface must only be called from a
** single thread.  All open [database connections] must be closed and all
** other SQLite resources must be deallocated prior to invoking
** sqlite3_shutdown().
**
** Among other things, ^sqlite3_initialize() will invoke
** sqlite3_os_init().  Similarly, ^sqlite3_shutdown()
** will invoke sqlite3_os_end().
**
** ^The sqlite3_initialize() routine returns [SQLITE_OK] on success.
** ^If for some reason, sqlite3_initialize() is unable to initialize
** the library (perhaps it is unable to allocate a needed resource such
** as a mutex) it returns an [error code] other than [SQLITE_OK].
**
** ^The sqlite3_initialize() routine is called internally by many other
** SQLite interfaces so that an application usually does not need to
** invoke sqlite3_initialize() directly.  For example, [sqlite3_open()]
** calls sqlite3_initialize() so the SQLite library will be automatically
** initialized when [sqlite3_open()] is called if it has not be initialized
** already.  ^However, if SQLite is compiled with the [SQLITE_OMIT_AUTOINIT]
** compile-time option, then the automatic calls to sqlite3_initialize()
** are omitted and the application must call sqlite3_initialize() directly
** prior to using any other SQLite interface.  For maximum portability,
** it is recommended that applications always invoke sqlite3_initialize()
** directly prior to using any other SQLite interface.  Future releases
** of SQLite may require this.  In other words, the behavior exhibited
** when SQLite is compiled with [SQLITE_OMIT_AUTOINIT] might become the
** default behavior in some future release of SQLite.
**
** The sqlite3_os_init() routine does operating-system specific
** initialization of the SQLite library.  The sqlite3_os_end()
** routine undoes the effect of sqlite3_os_init().  Typical tasks
** performed by these routines include allocation or deallocation
** of static resources, initialization of global variables,
** setting up a default [sqlite3_vfs] module, or setting up
** a default configuration using [sqlite3_config()].
**
** The application should never invoke either sqlite3_os_init()
** or sqlite3_os_end() directly.  The application should only invoke
** sqlite3_initialize() and sqlite3_shutdown().  The sqlite3_os_init()
** interface is called automatically by sqlite3_initialize() and
** sqlite3_os_end() is called by sqlite3_shutdown().  Appropriate
** implementations for sqlite3_os_init() and sqlite3_os_end()
** are built into SQLite when it is compiled for Unix, Windows, or OS/2.
** When [custom builds | built for other platforms]
** (using the [SQLITE_OS_OTHER=1] compile-time
** option) the application must supply a suitable implementation for
** sqlite3_os_init() and sqlite3_os_end().  An application-supplied
** implementation of sqlite3_os_init() or sqlite3_os_end()
** must return [SQLITE_OK] on success and some other [error code] upon
** failure.
*/
SQLITE_API int sqlite3_initialize(void);
SQLITE_API int sqlite3_shutdown(void);
SQLITE_API int sqlite3_os_init(void);
SQLITE_API int sqlite3_os_end(void);

/*
** CAPI3REF: Configuring The SQLite Library
**
** The sqlite3_config() interface is used to make global configuration
** changes to SQLite in order to tune SQLite to the specific needs of
** the application.  The default configuration is recommended for most
** applications and so this routine is usually not necessary.  It is
** provided to support rare applications with unusual needs.
**
** The sqlite3_config() interface is not threadsafe.  The application
** must insure that no other SQLite interfaces are invoked by other
** threads while sqlite3_config() is running.  Furthermore, sqlite3_config()
** may only be invoked prior to library initialization using
** [sqlite3_initialize()] or after shutdown by [sqlite3_shutdown()].
** ^If sqlite3_config() is called after [sqlite3_initialize()] and before
** [sqlite3_shutdown()] then it will return SQLITE_MISUSE.
** Note, however, that ^sqlite3_config() can be called as part of the
** implementation of an application-defined [sqlite3_os_init()].
**
** The first argument to sqlite3_config() is an integer
** [SQLITE_CONFIG_SINGLETHREAD | configuration option] that determines
** what property of SQLite is to be configured.  Subsequent arguments
** vary depending on the [SQLITE_CONFIG_SINGLETHREAD | configuration option]
** in the first argument.
**
** ^When a configuration option is set, sqlite3_config() returns [SQLITE_OK].
** ^If the option is unknown or SQLite is unable to set the option
** then this routine returns a non-zero [error code].
*/
SQLITE_API int sqlite3_config(int, ...);

/*
** CAPI3REF: Configure database connections
**
** The sqlite3_db_config() interface is used to make configuration
** changes to a [database connection].  The interface is similar to
** [sqlite3_config()] except that the changes apply to a single
** [database connection] (specified in the first argument).
**
** The second argument to sqlite3_db_config(D,V,...)  is the
** [SQLITE_DBCONFIG_LOOKASIDE | configuration verb] - an integer code 
** that indicates what aspect of the [database connection] is being configured.
** Subsequent arguments vary depending on the configuration verb.
**
** ^Calls to sqlite3_db_config() return SQLITE_OK if and only if
** the call is considered successful.
*/
SQLITE_API int sqlite3_db_config(sqlite3*, int op, ...);

/*
** CAPI3REF: Memory Allocation Routines
**
** An instance of this object defines the interface between SQLite
** and low-level memory allocation routines.
**
** This object is used in only one place in the SQLite interface.
** A pointer to an instance of this object is the argument to
** [sqlite3_config()] when the configuration option is
** [SQLITE_CONFIG_MALLOC] or [SQLITE_CONFIG_GETMALLOC].  
** By creating an instance of this object
** and passing it to [sqlite3_config]([SQLITE_CONFIG_MALLOC])
** during configuration, an application can specify an alternative
** memory allocation subsystem for SQLite to use for all of its
** dynamic memory needs.
**
** Note that SQLite comes with several [built-in memory allocators]
** that are perfectly adequate for the overwhelming majority of applications
** and that this object is only useful to a tiny minority of applications
** with specialized memory allocation requirements.  This object is
** also used during testing of SQLite in order to specify an alternative
** memory allocator that simulates memory out-of-memory conditions in
** order to verify that SQLite recovers gracefully from such
** conditions.
**
** The xMalloc and xFree methods must work like the
** malloc() and free() functions from the standard C library.
** The xRealloc method must work like realloc() from the standard C library
** with the exception that if the second argument to xRealloc is zero,
** xRealloc must be a no-op - it must not perform any allocation or
** deallocation.  ^SQLite guarantees that the second argument to
** xRealloc is always a value returned by a prior call to xRoundup.
** And so in cases where xRoundup always returns a positive number,
** xRealloc can perform exactly as the standard library realloc() and
** still be in compliance with this specification.
**
** xSize should return the allocated size of a memory allocation
** previously obtained from xMalloc or xRealloc.  The allocated size
** is always at least as big as the requested size but may be larger.
**
** The xRoundup method returns what would be the allocated size of
** a memory allocation given a particular requested size.  Most memory
** allocators round up memory allocations at least to the next multiple
** of 8.  Some allocators round up to a larger multiple or to a power of 2.
** Every memory allocation request coming in through [sqlite3_malloc()]
** or [sqlite3_realloc()] first calls xRoundup.  If xRoundup returns 0, 
** that causes the corresponding memory allocation to fail.
**
** The xInit method initializes the memory allocator.  (For example,
** it might allocate any require mutexes or initialize internal data
** structures.  The xShutdown method is invoked (indirectly) by
** [sqlite3_shutdown()] and should deallocate any resources acquired
** by xInit.  The pAppData pointer is used as the only parameter to
** xInit and xShutdown.
**
** SQLite holds the [SQLITE_MUTEX_STATIC_MASTER] mutex when it invokes
** the xInit method, so the xInit method need not be threadsafe.  The
** xShutdown method is only called from [sqlite3_shutdown()] so it does
** not need to be threadsafe either.  For all other methods, SQLite
** holds the [SQLITE_MUTEX_STATIC_MEM] mutex as long as the
** [SQLITE_CONFIG_MEMSTATUS] configuration option is turned on (which
** it is by default) and so the methods are automatically serialized.
** However, if [SQLITE_CONFIG_MEMSTATUS] is disabled, then the other
** methods must be threadsafe or else make their own arrangements for
** serialization.
**
** SQLite will never invoke xInit() more than once without an intervening
** call to xShutdown().
*/
typedef struct sqlite3_mem_methods sqlite3_mem_methods;
struct sqlite3_mem_methods {
  void *(*xMalloc)(int);         /* Memory allocation function */
  void (*xFree)(void*);          /* Free a prior allocation */
  void *(*xRealloc)(void*,int);  /* Resize an allocation */
  int (*xSize)(void*);           /* Return the size of an allocation */
  int (*xRoundup)(int);          /* Round up request size to allocation size */
  int (*xInit)(void*);           /* Initialize the memory allocator */
  void (*xShutdown)(void*);      /* Deinitialize the memory allocator */
  void *pAppData;                /* Argument to xInit() and xShutdown() */
};

/*
** CAPI3REF: Configuration Options
**
** These constants are the available integer configuration options that
** can be passed as the first argument to the [sqlite3_config()] interface.
**
** New configuration options may be added in future releases of SQLite.
** Existing configuration options might be discontinued.  Applications
** should check the return code from [sqlite3_config()] to make sure that
** the call worked.  The [sqlite3_config()] interface will return a
** non-zero [error code] if a discontinued or unsupported configuration option
** is invoked.
**
** <dl>
** <dt>SQLITE_CONFIG_SINGLETHREAD</dt>
** <dd>There are no arguments to this option.  ^This option sets the
** [threading mode] to Single-thread.  In other words, it disables
** all mutexing and puts SQLite into a mode where it can only be used
** by a single thread.   ^If SQLite is compiled with
** the [SQLITE_THREADSAFE | SQLITE_THREADSAFE=0] compile-time option then
** it is not possible to change the [threading mode] from its default
** value of Single-thread and so [sqlite3_config()] will return 
** [SQLITE_ERROR] if called with the SQLITE_CONFIG_SINGLETHREAD
** configuration option.</dd>
**
** <dt>SQLITE_CONFIG_MULTITHREAD</dt>
** <dd>There are no arguments to this option.  ^This option sets the
** [threading mode] to Multi-thread.  In other words, it disables
** mutexing on [database connection] and [prepared statement] objects.
** The application is responsible for serializing access to
** [database connections] and [prepared statements].  But other mutexes
** are enabled so that SQLite will be safe to use in a multi-threaded
** environment as long as no two threads attempt to use the same
** [database connection] at the same time.  ^If SQLite is compiled with
** the [SQLITE_THREADSAFE | SQLITE_THREADSAFE=0] compile-time option then
** it is not possible to set the Multi-thread [threading mode] and
** [sqlite3_config()] will return [SQLITE_ERROR] if called with the
** SQLITE_CONFIG_MULTITHREAD configuration option.</dd>
**
** <dt>SQLITE_CONFIG_SERIALIZED</dt>
** <dd>There are no arguments to this option.  ^This option sets the
** [threading mode] to Serialized. In other words, this option enables
** all mutexes including the recursive
** mutexes on [database connection] and [prepared statement] objects.
** In this mode (which is the default when SQLite is compiled with
** [SQLITE_THREADSAFE=1]) the SQLite library will itself serialize access
** to [database connections] and [prepared statements] so that the
** application is free to use the same [database connection] or the
** same [prepared statement] in different threads at the same time.
** ^If SQLite is compiled with
** the [SQLITE_THREADSAFE | SQLITE_THREADSAFE=0] compile-time option then
** it is not possible to set the Serialized [threading mode] and
** [sqlite3_config()] will return [SQLITE_ERROR] if called with the
** SQLITE_CONFIG_SERIALIZED configuration option.</dd>
**
** <dt>SQLITE_CONFIG_MALLOC</dt>
** <dd> ^(This option takes a single argument which is a pointer to an
** instance of the [sqlite3_mem_methods] structure.  The argument specifies
** alternative low-level memory allocation routines to be used in place of
** the memory allocation routines built into SQLite.)^ ^SQLite makes
** its own private copy of the content of the [sqlite3_mem_methods] structure
** before the [sqlite3_config()] call returns.</dd>
**
** <dt>SQLITE_CONFIG_GETMALLOC</dt>
** <dd> ^(This option takes a single argument which is a pointer to an
** instance of the [sqlite3_mem_methods] structure.  The [sqlite3_mem_methods]
** structure is filled with the currently defined memory allocation routines.)^
** This option can be used to overload the default memory allocation
** routines with a wrapper that simulations memory allocation failure or
** tracks memory usage, for example. </dd>
**
** <dt>SQLITE_CONFIG_MEMSTATUS</dt>
** <dd> ^This option takes single argument of type int, interpreted as a 
** boolean, which enables or disables the collection of memory allocation 
** statistics. ^(When memory allocation statistics are disabled, the 
** following SQLite interfaces become non-operational:
**   <ul>
**   <li> [sqlite3_memory_used()]
**   <li> [sqlite3_memory_highwater()]
**   <li> [sqlite3_soft_heap_limit64()]
**   <li> [sqlite3_status()]
**   </ul>)^
** ^Memory allocation statistics are enabled by default unless SQLite is
** compiled with [SQLITE_DEFAULT_MEMSTATUS]=0 in which case memory
** allocation statistics are disabled by default.
** </dd>
**
** <dt>SQLITE_CONFIG_SCRATCH</dt>
** <dd> ^This option specifies a static memory buffer that SQLite can use for
** scratch memory.  There are three arguments:  A pointer an 8-byte
** aligned memory buffer from which the scratch allocations will be
** drawn, the size of each scratch allocation (sz),
** and the maximum number of scratch allocations (N).  The sz
** argument must be a multiple of 16.
** The first argument must be a pointer to an 8-byte aligned buffer
** of at least sz*N bytes of memory.
** ^SQLite will use no more than two scratch buffers per thread.  So
** N should be set to twice the expected maximum number of threads.
** ^SQLite will never require a scratch buffer that is more than 6
** times the database page size. ^If SQLite needs needs additional
** scratch memory beyond what is provided by this configuration option, then 
** [sqlite3_malloc()] will be used to obtain the memory needed.</dd>
**
** <dt>SQLITE_CONFIG_PAGECACHE</dt>
** <dd> ^This option specifies a static memory buffer that SQLite can use for
** the database page cache with the default page cache implemenation.  
** This configuration should not be used if an application-define page
** cache implementation is loaded using the SQLITE_CONFIG_PCACHE option.
** There are three arguments to this option: A pointer to 8-byte aligned
** memory, the size of each page buffer (sz), and the number of pages (N).
** The sz argument should be the size of the largest database page
** (a power of two between 512 and 32768) plus a little extra for each
** page header.  ^The page header size is 20 to 40 bytes depending on
** the host architecture.  ^It is harmless, apart from the wasted memory,
** to make sz a little too large.  The first
** argument should point to an allocation of at least sz*N bytes of memory.
** ^SQLite will use the memory provided by the first argument to satisfy its
** memory needs for the first N pages that it adds to cache.  ^If additional
** page cache memory is needed beyond what is provided by this option, then
** SQLite goes to [sqlite3_malloc()] for the additional storage space.
** The pointer in the first argument must
** be aligned to an 8-byte boundary or subsequent behavior of SQLite
** will be undefined.</dd>
**
** <dt>SQLITE_CONFIG_HEAP</dt>
** <dd> ^This option specifies a static memory buffer that SQLite will use
** for all of its dynamic memory allocation needs beyond those provided
** for by [SQLITE_CONFIG_SCRATCH] and [SQLITE_CONFIG_PAGECACHE].
** There are three arguments: An 8-byte aligned pointer to the memory,
** the number of bytes in the memory buffer, and the minimum allocation size.
** ^If the first pointer (the memory pointer) is NULL, then SQLite reverts
** to using its default memory allocator (the system malloc() implementation),
** undoing any prior invocation of [SQLITE_CONFIG_MALLOC].  ^If the
** memory pointer is not NULL and either [SQLITE_ENABLE_MEMSYS3] or
** [SQLITE_ENABLE_MEMSYS5] are defined, then the alternative memory
** allocator is engaged to handle all of SQLites memory allocation needs.
** The first pointer (the memory pointer) must be aligned to an 8-byte
** boundary or subsequent behavior of SQLite will be undefined.
** The minimum allocation size is capped at 2^12. Reasonable values
** for the minimum allocation size are 2^5 through 2^8.</dd>
**
** <dt>SQLITE_CONFIG_MUTEX</dt>
** <dd> ^(This option takes a single argument which is a pointer to an
** instance of the [sqlite3_mutex_methods] structure.  The argument specifies
** alternative low-level mutex routines to be used in place
** the mutex routines built into SQLite.)^  ^SQLite makes a copy of the
** content of the [sqlite3_mutex_methods] structure before the call to
** [sqlite3_config()] returns. ^If SQLite is compiled with
** the [SQLITE_THREADSAFE | SQLITE_THREADSAFE=0] compile-time option then
** the entire mutexing subsystem is omitted from the build and hence calls to
** [sqlite3_config()] with the SQLITE_CONFIG_MUTEX configuration option will
** return [SQLITE_ERROR].</dd>
**
** <dt>SQLITE_CONFIG_GETMUTEX</dt>
** <dd> ^(This option takes a single argument which is a pointer to an
** instance of the [sqlite3_mutex_methods] structure.  The
** [sqlite3_mutex_methods]
** structure is filled with the currently defined mutex routines.)^
** This option can be used to overload the default mutex allocation
** routines with a wrapper used to track mutex usage for performance
** profiling or testing, for example.   ^If SQLite is compiled with
** the [SQLITE_THREADSAFE | SQLITE_THREADSAFE=0] compile-time option then
** the entire mutexing subsystem is omitted from the build and hence calls to
** [sqlite3_config()] with the SQLITE_CONFIG_GETMUTEX configuration option will
** return [SQLITE_ERROR].</dd>
**
** <dt>SQLITE_CONFIG_LOOKASIDE</dt>
** <dd> ^(This option takes two arguments that determine the default
** memory allocation for the lookaside memory allocator on each
** [database connection].  The first argument is the
** size of each lookaside buffer slot and the second is the number of
** slots allocated to each database connection.)^  ^(This option sets the
** <i>default</i> lookaside size. The [SQLITE_DBCONFIG_LOOKASIDE]
** verb to [sqlite3_db_config()] can be used to change the lookaside
** configuration on individual connections.)^ </dd>
**
** <dt>SQLITE_CONFIG_PCACHE</dt>
** <dd> ^(This option takes a single argument which is a pointer to
** an [sqlite3_pcache_methods] object.  This object specifies the interface
** to a custom page cache implementation.)^  ^SQLite makes a copy of the
** object and uses it for page cache memory allocations.</dd>
**
** <dt>SQLITE_CONFIG_GETPCACHE</dt>
** <dd> ^(This option takes a single argument which is a pointer to an
** [sqlite3_pcache_methods] object.  SQLite copies of the current
** page cache implementation into that object.)^ </dd>
**
** <dt>SQLITE_CONFIG_LOG</dt>
** <dd> ^The SQLITE_CONFIG_LOG option takes two arguments: a pointer to a
** function with a call signature of void(*)(void*,int,const char*), 
** and a pointer to void. ^If the function pointer is not NULL, it is
** invoked by [sqlite3_log()] to process each logging event.  ^If the
** function pointer is NULL, the [sqlite3_log()] interface becomes a no-op.
** ^The void pointer that is the second argument to SQLITE_CONFIG_LOG is
** passed through as the first parameter to the application-defined logger
** function whenever that function is invoked.  ^The second parameter to
** the logger function is a copy of the first parameter to the corresponding
** [sqlite3_log()] call and is intended to be a [result code] or an
** [extended result code].  ^The third parameter passed to the logger is
** log message after formatting via [sqlite3_snprintf()].
** The SQLite logging interface is not reentrant; the logger function
** supplied by the application must not invoke any SQLite interface.
** In a multi-threaded application, the application-defined logger
** function must be threadsafe. </dd>
**
** </dl>
*/
#define SQLITE_CONFIG_SINGLETHREAD  1  /* nil */
#define SQLITE_CONFIG_MULTITHREAD   2  /* nil */
#define SQLITE_CONFIG_SERIALIZED    3  /* nil */
#define SQLITE_CONFIG_MALLOC        4  /* sqlite3_mem_methods* */
#define SQLITE_CONFIG_GETMALLOC     5  /* sqlite3_mem_methods* */
#define SQLITE_CONFIG_SCRATCH       6  /* void*, int sz, int N */
#define SQLITE_CONFIG_PAGECACHE     7  /* void*, int sz, int N */
#define SQLITE_CONFIG_HEAP          8  /* void*, int nByte, int min */
#define SQLITE_CONFIG_MEMSTATUS     9  /* boolean */
#define SQLITE_CONFIG_MUTEX        10  /* sqlite3_mutex_methods* */
#define SQLITE_CONFIG_GETMUTEX     11  /* sqlite3_mutex_methods* */
/* previously SQLITE_CONFIG_CHUNKALLOC 12 which is now unused. */ 
#define SQLITE_CONFIG_LOOKASIDE    13  /* int int */
#define SQLITE_CONFIG_PCACHE       14  /* sqlite3_pcache_methods* */
#define SQLITE_CONFIG_GETPCACHE    15  /* sqlite3_pcache_methods* */
#define SQLITE_CONFIG_LOG          16  /* xFunc, void* */

/*
** CAPI3REF: Database Connection Configuration Options
**
** These constants are the available integer configuration options that
** can be passed as the second argument to the [sqlite3_db_config()] interface.
**
** New configuration options may be added in future releases of SQLite.
** Existing configuration options might be discontinued.  Applications
** should check the return code from [sqlite3_db_config()] to make sure that
** the call worked.  ^The [sqlite3_db_config()] interface will return a
** non-zero [error code] if a discontinued or unsupported configuration option
** is invoked.
**
** <dl>
** <dt>SQLITE_DBCONFIG_LOOKASIDE</dt>
** <dd> ^This option takes three additional arguments that determine the 
** [lookaside memory allocator] configuration for the [database connection].
** ^The first argument (the third parameter to [sqlite3_db_config()] is a
** pointer to a memory buffer to use for lookaside memory.
** ^The first argument after the SQLITE_DBCONFIG_LOOKASIDE verb
** may be NULL in which case SQLite will allocate the
** lookaside buffer itself using [sqlite3_malloc()]. ^The second argument is the
** size of each lookaside buffer slot.  ^The third argument is the number of
** slots.  The size of the buffer in the first argument must be greater than
** or equal to the product of the second and third arguments.  The buffer
** must be aligned to an 8-byte boundary.  ^If the second argument to
** SQLITE_DBCONFIG_LOOKASIDE is not a multiple of 8, it is internally
** rounded down to the next smaller multiple of 8.  ^(The lookaside memory
** configuration for a database connection can only be changed when that
** connection is not currently using lookaside memory, or in other words
** when the "current value" returned by
** [sqlite3_db_status](D,[SQLITE_CONFIG_LOOKASIDE],...) is zero.
** Any attempt to change the lookaside memory configuration when lookaside
** memory is in use leaves the configuration unchanged and returns 
** [SQLITE_BUSY].)^</dd>
**
** <dt>SQLITE_DBCONFIG_ENABLE_FKEY</dt>
** <dd> ^This option is used to enable or disable the enforcement of
** [foreign key constraints].  There should be two additional arguments.
** The first argument is an integer which is 0 to disable FK enforcement,
** positive to enable FK enforcement or negative to leave FK enforcement
** unchanged.  The second parameter is a pointer to an integer into which
** is written 0 or 1 to indicate whether FK enforcement is off or on
** following this call.  The second parameter may be a NULL pointer, in
** which case the FK enforcement setting is not reported back. </dd>
**
** <dt>SQLITE_DBCONFIG_ENABLE_TRIGGER</dt>
** <dd> ^This option is used to enable or disable [CREATE TRIGGER | triggers].
** There should be two additional arguments.
** The first argument is an integer which is 0 to disable triggers,
** positive to enable triggers or negative to leave the setting unchanged.
** The second parameter is a pointer to an integer into which
** is written 0 or 1 to indicate whether triggers are disabled or enabled
** following this call.  The second parameter may be a NULL pointer, in
** which case the trigger setting is not reported back. </dd>
**
** </dl>
*/
#define SQLITE_DBCONFIG_LOOKASIDE       1001  /* void* int int */
#define SQLITE_DBCONFIG_ENABLE_FKEY     1002  /* int int* */
#define SQLITE_DBCONFIG_ENABLE_TRIGGER  1003  /* int int* */


/*
** CAPI3REF: Enable Or Disable Extended Result Codes
**
** ^The sqlite3_extended_result_codes() routine enables or disables the
** [extended result codes] feature of SQLite. ^The extended result
** codes are disabled by default for historical compatibility.
*/
SQLITE_API int sqlite3_extended_result_codes(sqlite3*, int onoff);

/*
** CAPI3REF: Last Insert Rowid
**
** ^Each entry in an SQLite table has a unique 64-bit signed
** integer key called the [ROWID | "rowid"]. ^The rowid is always available
** as an undeclared column named ROWID, OID, or _ROWID_ as long as those
** names are not also used by explicitly declared columns. ^If
** the table has a column of type [INTEGER PRIMARY KEY] then that column
** is another alias for the rowid.
**
** ^This routine returns the [rowid] of the most recent
** successful [INSERT] into the database from the [database connection]
** in the first argument.  ^If no successful [INSERT]s
** have ever occurred on that database connection, zero is returned.
**
** ^(If an [INSERT] occurs within a trigger, then the [rowid] of the inserted
** row is returned by this routine as long as the trigger is running.
** But once the trigger terminates, the value returned by this routine
** reverts to the last value inserted before the trigger fired.)^
**
** ^An [INSERT] that fails due to a constraint violation is not a
** successful [INSERT] and does not change the value returned by this
** routine.  ^Thus INSERT OR FAIL, INSERT OR IGNORE, INSERT OR ROLLBACK,
** and INSERT OR ABORT make no changes to the return value of this
** routine when their insertion fails.  ^(When INSERT OR REPLACE
** encounters a constraint violation, it does not fail.  The
** INSERT continues to completion after deleting rows that caused
** the constraint problem so INSERT OR REPLACE will always change
** the return value of this interface.)^
**
** ^For the purposes of this routine, an [INSERT] is considered to
** be successful even if it is subsequently rolled back.
**
** This function is accessible to SQL statements via the
** [last_insert_rowid() SQL function].
**
** If a separate thread performs a new [INSERT] on the same
** database connection while the [sqlite3_last_insert_rowid()]
** function is running and thus changes the last insert [rowid],
** then the value returned by [sqlite3_last_insert_rowid()] is
** unpredictable and might not equal either the old or the new
** last insert [rowid].
*/
SQLITE_API sqlite3_int64 sqlite3_last_insert_rowid(sqlite3*);

/*
** CAPI3REF: Count The Number Of Rows Modified
**
** ^This function returns the number of database rows that were changed
** or inserted or deleted by the most recently completed SQL statement
** on the [database connection] specified by the first parameter.
** ^(Only changes that are directly specified by the [INSERT], [UPDATE],
** or [DELETE] statement are counted.  Auxiliary changes caused by
** triggers or [foreign key actions] are not counted.)^ Use the
** [sqlite3_total_changes()] function to find the total number of changes
** including changes caused by triggers and foreign key actions.
**
** ^Changes to a view that are simulated by an [INSTEAD OF trigger]
** are not counted.  Only real table changes are counted.
**
** ^(A "row change" is a change to a single row of a single table
** caused by an INSERT, DELETE, or UPDATE statement.  Rows that
** are changed as side effects of [REPLACE] constraint resolution,
** rollback, ABORT processing, [DROP TABLE], or by any other
** mechanisms do not count as direct row changes.)^
**
** A "trigger context" is a scope of execution that begins and
** ends with the script of a [CREATE TRIGGER | trigger]. 
** Most SQL statements are
** evaluated outside of any trigger.  This is the "top level"
** trigger context.  If a trigger fires from the top level, a
** new trigger context is entered for the duration of that one
** trigger.  Subtriggers create subcontexts for their duration.
**
** ^Calling [sqlite3_exec()] or [sqlite3_step()] recursively does
** not create a new trigger context.
**
** ^This function returns the number of direct row changes in the
** most recent INSERT, UPDATE, or DELETE statement within the same
** trigger context.
**
** ^Thus, when called from the top level, this function returns the
** number of changes in the most recent INSERT, UPDATE, or DELETE
** that also occurred at the top level.  ^(Within the body of a trigger,
** the sqlite3_changes() interface can be called to find the number of
** changes in the most recently completed INSERT, UPDATE, or DELETE
** statement within the body of the same trigger.
** However, the number returned does not include changes
** caused by subtriggers since those have their own context.)^
**
** See also the [sqlite3_total_changes()] interface, the
** [count_changes pragma], and the [changes() SQL function].
**
** If a separate thread makes changes on the same database connection
** while [sqlite3_changes()] is running then the value returned
** is unpredictable and not meaningful.
*/
SQLITE_API int sqlite3_changes(sqlite3*);

/*
** CAPI3REF: Total Number Of Rows Modified
**
** ^This function returns the number of row changes caused by [INSERT],
** [UPDATE] or [DELETE] statements since the [database connection] was opened.
** ^(The count returned by sqlite3_total_changes() includes all changes
** from all [CREATE TRIGGER | trigger] contexts and changes made by
** [foreign key actions]. However,
** the count does not include changes used to implement [REPLACE] constraints,
** do rollbacks or ABORT processing, or [DROP TABLE] processing.  The
** count does not include rows of views that fire an [INSTEAD OF trigger],
** though if the INSTEAD OF trigger makes changes of its own, those changes 
** are counted.)^
** ^The sqlite3_total_changes() function counts the changes as soon as
** the statement that makes them is completed (when the statement handle
** is passed to [sqlite3_reset()] or [sqlite3_finalize()]).
**
** See also the [sqlite3_changes()] interface, the
** [count_changes pragma], and the [total_changes() SQL function].
**
** If a separate thread makes changes on the same database connection
** while [sqlite3_total_changes()] is running then the value
** returned is unpredictable and not meaningful.
*/
SQLITE_API int sqlite3_total_changes(sqlite3*);

/*
** CAPI3REF: Interrupt A Long-Running Query
**
** ^This function causes any pending database operation to abort and
** return at its earliest opportunity. This routine is typically
** called in response to a user action such as pressing "Cancel"
** or Ctrl-C where the user wants a long query operation to halt
** immediately.
**
** ^It is safe to call this routine from a thread different from the
** thread that is currently running the database operation.  But it
** is not safe to call this routine with a [database connection] that
** is closed or might close before sqlite3_interrupt() returns.
**
** ^If an SQL operation is very nearly finished at the time when
** sqlite3_interrupt() is called, then it might not have an opportunity
** to be interrupted and might continue to completion.
**
** ^An SQL operation that is interrupted will return [SQLITE_INTERRUPT].
** ^If the interrupted SQL operation is an INSERT, UPDATE, or DELETE
** that is inside an explicit transaction, then the entire transaction
** will be rolled back automatically.
**
** ^The sqlite3_interrupt(D) call is in effect until all currently running
** SQL statements on [database connection] D complete.  ^Any new SQL statements
** that are started after the sqlite3_interrupt() call and before the 
** running statements reaches zero are interrupted as if they had been
** running prior to the sqlite3_interrupt() call.  ^New SQL statements
** that are started after the running statement count reaches zero are
** not effected by the sqlite3_interrupt().
** ^A call to sqlite3_interrupt(D) that occurs when there are no running
** SQL statements is a no-op and has no effect on SQL statements
** that are started after the sqlite3_interrupt() call returns.
**
** If the database connection closes while [sqlite3_interrupt()]
** is running then bad things will likely happen.
*/
SQLITE_API void sqlite3_interrupt(sqlite3*);

/*
** CAPI3REF: Determine If An SQL Statement Is Complete
**
** These routines are useful during command-line input to determine if the
** currently entered text seems to form a complete SQL statement or
** if additional input is needed before sending the text into
** SQLite for parsing.  ^These routines return 1 if the input string
** appears to be a complete SQL statement.  ^A statement is judged to be
** complete if it ends with a semicolon token and is not a prefix of a
** well-formed CREATE TRIGGER statement.  ^Semicolons that are embedded within
** string literals or quoted identifier names or comments are not
** independent tokens (they are part of the token in which they are
** embedded) and thus do not count as a statement terminator.  ^Whitespace
** and comments that follow the final semicolon are ignored.
**
** ^These routines return 0 if the statement is incomplete.  ^If a
** memory allocation fails, then SQLITE_NOMEM is returned.
**
** ^These routines do not parse the SQL statements thus
** will not detect syntactically incorrect SQL.
**
** ^(If SQLite has not been initialized using [sqlite3_initialize()] prior 
** to invoking sqlite3_complete16() then sqlite3_initialize() is invoked
** automatically by sqlite3_complete16().  If that initialization fails,
** then the return value from sqlite3_complete16() will be non-zero
** regardless of whether or not the input SQL is complete.)^
**
** The input to [sqlite3_complete()] must be a zero-terminated
** UTF-8 string.
**
** The input to [sqlite3_complete16()] must be a zero-terminated
** UTF-16 string in native byte order.
*/
SQLITE_API int sqlite3_complete(const char *sql);
SQLITE_API int sqlite3_complete16(const void *sql);

/*
** CAPI3REF: Register A Callback To Handle SQLITE_BUSY Errors
**
** ^This routine sets a callback function that might be invoked whenever
** an attempt is made to open a database table that another thread
** or process has locked.
**
** ^If the busy callback is NULL, then [SQLITE_BUSY] or [SQLITE_IOERR_BLOCKED]
** is returned immediately upon encountering the lock.  ^If the busy callback
** is not NULL, then the callback might be invoked with two arguments.
**
** ^The first argument to the busy handler is a copy of the void* pointer which
** is the third argument to sqlite3_busy_handler().  ^The second argument to
** the busy handler callback is the number of times that the busy handler has
** been invoked for this locking event.  ^If the
** busy callback returns 0, then no additional attempts are made to
** access the database and [SQLITE_BUSY] or [SQLITE_IOERR_BLOCKED] is returned.
** ^If the callback returns non-zero, then another attempt
** is made to open the database for reading and the cycle repeats.
**
** The presence of a busy handler does not guarantee that it will be invoked
** when there is lock contention. ^If SQLite determines that invoking the busy
** handler could result in a deadlock, it will go ahead and return [SQLITE_BUSY]
** or [SQLITE_IOERR_BLOCKED] instead of invoking the busy handler.
** Consider a scenario where one process is holding a read lock that
** it is trying to promote to a reserved lock and
** a second process is holding a reserved lock that it is trying
** to promote to an exclusive lock.  The first process cannot proceed
** because it is blocked by the second and the second process cannot
** proceed because it is blocked by the first.  If both processes
** invoke the busy handlers, neither will make any progress.  Therefore,
** SQLite returns [SQLITE_BUSY] for the first process, hoping that this
** will induce the first process to release its read lock and allow
** the second process to proceed.
**
** ^The default busy callback is NULL.
**
** ^The [SQLITE_BUSY] error is converted to [SQLITE_IOERR_BLOCKED]
** when SQLite is in the middle of a large transaction where all the
** changes will not fit into the in-memory cache.  SQLite will
** already hold a RESERVED lock on the database file, but it needs
** to promote this lock to EXCLUSIVE so that it can spill cache
** pages into the database file without harm to concurrent
** readers.  ^If it is unable to promote the lock, then the in-memory
** cache will be left in an inconsistent state and so the error
** code is promoted from the relatively benign [SQLITE_BUSY] to
** the more severe [SQLITE_IOERR_BLOCKED].  ^This error code promotion
** forces an automatic rollback of the changes.  See the
** <a href="/cvstrac/wiki?p=CorruptionFollowingBusyError">
** CorruptionFollowingBusyError</a> wiki page for a discussion of why
** this is important.
**
** ^(There can only be a single busy handler defined for each
** [database connection].  Setting a new busy handler clears any
** previously set handler.)^  ^Note that calling [sqlite3_busy_timeout()]
** will also set or clear the busy handler.
**
** The busy callback should not take any actions which modify the
** database connection that invoked the busy handler.  Any such actions
** result in undefined behavior.
** 
** A busy handler must not close the database connection
** or [prepared statement] that invoked the busy handler.
*/
SQLITE_API int sqlite3_busy_handler(sqlite3*, int(*)(void*,int), void*);

/*
** CAPI3REF: Set A Busy Timeout
**
** ^This routine sets a [sqlite3_busy_handler | busy handler] that sleeps
** for a specified amount of time when a table is locked.  ^The handler
** will sleep multiple times until at least "ms" milliseconds of sleeping
** have accumulated.  ^After at least "ms" milliseconds of sleeping,
** the handler returns 0 which causes [sqlite3_step()] to return
** [SQLITE_BUSY] or [SQLITE_IOERR_BLOCKED].
**
** ^Calling this routine with an argument less than or equal to zero
** turns off all busy handlers.
**
** ^(There can only be a single busy handler for a particular
** [database connection] any any given moment.  If another busy handler
** was defined  (using [sqlite3_busy_handler()]) prior to calling
** this routine, that other busy handler is cleared.)^
*/
SQLITE_API int sqlite3_busy_timeout(sqlite3*, int ms);

/*
** CAPI3REF: Convenience Routines For Running Queries
**
** This is a legacy interface that is preserved for backwards compatibility.
** Use of this interface is not recommended.
**
** Definition: A <b>result table</b> is memory data structure created by the
** [sqlite3_get_table()] interface.  A result table records the
** complete query results from one or more queries.
**
** The table conceptually has a number of rows and columns.  But
** these numbers are not part of the result table itself.  These
** numbers are obtained separately.  Let N be the number of rows
** and M be the number of columns.
**
** A result table is an array of pointers to zero-terminated UTF-8 strings.
** There are (N+1)*M elements in the array.  The first M pointers point
** to zero-terminated strings that  contain the names of the columns.
** The remaining entries all point to query results.  NULL values result
** in NULL pointers.  All other values are in their UTF-8 zero-terminated
** string representation as returned by [sqlite3_column_text()].
**
** A result table might consist of one or more memory allocations.
** It is not safe to pass a result table directly to [sqlite3_free()].
** A result table should be deallocated using [sqlite3_free_table()].
**
** ^(As an example of the result table format, suppose a query result
** is as follows:
**
** <blockquote><pre>
**        Name        | Age
**        -----------------------
**        Alice       | 43
**        Bob         | 28
**        Cindy       | 21
** </pre></blockquote>
**
** There are two column (M==2) and three rows (N==3).  Thus the
** result table has 8 entries.  Suppose the result table is stored
** in an array names azResult.  Then azResult holds this content:
**
** <blockquote><pre>
**        azResult&#91;0] = "Name";
**        azResult&#91;1] = "Age";
**        azResult&#91;2] = "Alice";
**        azResult&#91;3] = "43";
**        azResult&#91;4] = "Bob";
**        azResult&#91;5] = "28";
**        azResult&#91;6] = "Cindy";
**        azResult&#91;7] = "21";
** </pre></blockquote>)^
**
** ^The sqlite3_get_table() function evaluates one or more
** semicolon-separated SQL statements in the zero-terminated UTF-8
** string of its 2nd parameter and returns a result table to the
** pointer given in its 3rd parameter.
**
** After the application has finished with the result from sqlite3_get_table(),
** it must pass the result table pointer to sqlite3_free_table() in order to
** release the memory that was malloced.  Because of the way the
** [sqlite3_malloc()] happens within sqlite3_get_table(), the calling
** function must not try to call [sqlite3_free()] directly.  Only
** [sqlite3_free_table()] is able to release the memory properly and safely.
**
** The sqlite3_get_table() interface is implemented as a wrapper around
** [sqlite3_exec()].  The sqlite3_get_table() routine does not have access
** to any internal data structures of SQLite.  It uses only the public
** interface defined here.  As a consequence, errors that occur in the
** wrapper layer outside of the internal [sqlite3_exec()] call are not
** reflected in subsequent calls to [sqlite3_errcode()] or
** [sqlite3_errmsg()].
*/
SQLITE_API int sqlite3_get_table(
  sqlite3 *db,          /* An open database */
  const char *zSql,     /* SQL to be evaluated */
  char ***pazResult,    /* Results of the query */
  int *pnRow,           /* Number of result rows written here */
  int *pnColumn,        /* Number of result columns written here */
  char **pzErrmsg       /* Error msg written here */
);
SQLITE_API void sqlite3_free_table(char **result);

/*
** CAPI3REF: Formatted String Printing Functions
**
** These routines are work-alikes of the "printf()" family of functions
** from the standard C library.
**
** ^The sqlite3_mprintf() and sqlite3_vmprintf() routines write their
** results into memory obtained from [sqlite3_malloc()].
** The strings returned by these two routines should be
** released by [sqlite3_free()].  ^Both routines return a
** NULL pointer if [sqlite3_malloc()] is unable to allocate enough
** memory to hold the resulting string.
**
** ^(The sqlite3_snprintf() routine is similar to "snprintf()" from
** the standard C library.  The result is written into the
** buffer supplied as the second parameter whose size is given by
** the first parameter. Note that the order of the
** first two parameters is reversed from snprintf().)^  This is an
** historical accident that cannot be fixed without breaking
** backwards compatibility.  ^(Note also that sqlite3_snprintf()
** returns a pointer to its buffer instead of the number of
** characters actually written into the buffer.)^  We admit that
** the number of characters written would be a more useful return
** value but we cannot change the implementation of sqlite3_snprintf()
** now without breaking compatibility.
**
** ^As long as the buffer size is greater than zero, sqlite3_snprintf()
** guarantees that the buffer is always zero-terminated.  ^The first
** parameter "n" is the total size of the buffer, including space for
** the zero terminator.  So the longest string that can be completely
** written will be n-1 characters.
**
** ^The sqlite3_vsnprintf() routine is a varargs version of sqlite3_snprintf().
**
** These routines all implement some additional formatting
** options that are useful for constructing SQL statements.
** All of the usual printf() formatting options apply.  In addition, there
** is are "%q", "%Q", and "%z" options.
**
** ^(The %q option works like %s in that it substitutes a null-terminated
** string from the argument list.  But %q also doubles every '\'' character.
** %q is designed for use inside a string literal.)^  By doubling each '\''
** character it escapes that character and allows it to be inserted into
** the string.
**
** For example, assume the string variable zText contains text as follows:
**
** <blockquote><pre>
**  char *zText = "It's a happy day!";
** </pre></blockquote>
**
** One can use this text in an SQL statement as follows:
**
** <blockquote><pre>
**  char *zSQL = sqlite3_mprintf("INSERT INTO table VALUES('%q')", zText);
**  sqlite3_exec(db, zSQL, 0, 0, 0);
**  sqlite3_free(zSQL);
** </pre></blockquote>
**
** Because the %q format string is used, the '\'' character in zText
** is escaped and the SQL generated is as follows:
**
** <blockquote><pre>
**  INSERT INTO table1 VALUES('It''s a happy day!')
** </pre></blockquote>
**
** This is correct.  Had we used %s instead of %q, the generated SQL
** would have looked like this:
**
** <blockquote><pre>
**  INSERT INTO table1 VALUES('It's a happy day!');
** </pre></blockquote>
**
** This second example is an SQL syntax error.  As a general rule you should
** always use %q instead of %s when inserting text into a string literal.
**
** ^(The %Q option works like %q except it also adds single quotes around
** the outside of the total string.  Additionally, if the parameter in the
** argument list is a NULL pointer, %Q substitutes the text "NULL" (without
** single quotes).)^  So, for example, one could say:
**
** <blockquote><pre>
**  char *zSQL = sqlite3_mprintf("INSERT INTO table VALUES(%Q)", zText);
**  sqlite3_exec(db, zSQL, 0, 0, 0);
**  sqlite3_free(zSQL);
** </pre></blockquote>
**
** The code above will render a correct SQL statement in the zSQL
** variable even if the zText variable is a NULL pointer.
**
** ^(The "%z" formatting option works like "%s" but with the
** addition that after the string has been read and copied into
** the result, [sqlite3_free()] is called on the input string.)^
*/
SQLITE_API char *sqlite3_mprintf(const char*,...);
SQLITE_API char *sqlite3_vmprintf(const char*, va_list);
SQLITE_API char *sqlite3_snprintf(int,char*,const char*, ...);
SQLITE_API char *sqlite3_vsnprintf(int,char*,const char*, va_list);

/*
** CAPI3REF: Memory Allocation Subsystem
**
** The SQLite core uses these three routines for all of its own
** internal memory allocation needs. "Core" in the previous sentence
** does not include operating-system specific VFS implementation.  The
** Windows VFS uses native malloc() and free() for some operations.
**
** ^The sqlite3_malloc() routine returns a pointer to a block
** of memory at least N bytes in length, where N is the parameter.
** ^If sqlite3_malloc() is unable to obtain sufficient free
** memory, it returns a NULL pointer.  ^If the parameter N to
** sqlite3_malloc() is zero or negative then sqlite3_malloc() returns
** a NULL pointer.
**
** ^Calling sqlite3_free() with a pointer previously returned
** by sqlite3_malloc() or sqlite3_realloc() releases that memory so
** that it might be reused.  ^The sqlite3_free() routine is
** a no-op if is called with a NULL pointer.  Passing a NULL pointer
** to sqlite3_free() is harmless.  After being freed, memory
** should neither be read nor written.  Even reading previously freed
** memory might result in a segmentation fault or other severe error.
** Memory corruption, a segmentation fault, or other severe error
** might result if sqlite3_free() is called with a non-NULL pointer that
** was not obtained from sqlite3_malloc() or sqlite3_realloc().
**
** ^(The sqlite3_realloc() interface attempts to resize a
** prior memory allocation to be at least N bytes, where N is the
** second parameter.  The memory allocation to be resized is the first
** parameter.)^ ^ If the first parameter to sqlite3_realloc()
** is a NULL pointer then its behavior is identical to calling
** sqlite3_malloc(N) where N is the second parameter to sqlite3_realloc().
** ^If the second parameter to sqlite3_realloc() is zero or
** negative then the behavior is exactly the same as calling
** sqlite3_free(P) where P is the first parameter to sqlite3_realloc().
** ^sqlite3_realloc() returns a pointer to a memory allocation
** of at least N bytes in size or NULL if sufficient memory is unavailable.
** ^If M is the size of the prior allocation, then min(N,M) bytes
** of the prior allocation are copied into the beginning of buffer returned
** by sqlite3_realloc() and the prior allocation is freed.
** ^If sqlite3_realloc() returns NULL, then the prior allocation
** is not freed.
**
** ^The memory returned by sqlite3_malloc() and sqlite3_realloc()
** is always aligned to at least an 8 byte boundary, or to a
** 4 byte boundary if the [SQLITE_4_BYTE_ALIGNED_MALLOC] compile-time
** option is used.
**
** In SQLite version 3.5.0 and 3.5.1, it was possible to define
** the SQLITE_OMIT_MEMORY_ALLOCATION which would cause the built-in
** implementation of these routines to be omitted.  That capability
** is no longer provided.  Only built-in memory allocators can be used.
**
** The Windows OS interface layer calls
** the system malloc() and free() directly when converting
** filenames between the UTF-8 encoding used by SQLite
** and whatever filename encoding is used by the particular Windows
** installation.  Memory allocation errors are detected, but
** they are reported back as [SQLITE_CANTOPEN] or
** [SQLITE_IOERR] rather than [SQLITE_NOMEM].
**
** The pointer arguments to [sqlite3_free()] and [sqlite3_realloc()]
** must be either NULL or else pointers obtained from a prior
** invocation of [sqlite3_malloc()] or [sqlite3_realloc()] that have
** not yet been released.
**
** The application must not read or write any part of
** a block of memory after it has been released using
** [sqlite3_free()] or [sqlite3_realloc()].
*/
SQLITE_API void *sqlite3_malloc(int);
SQLITE_API void *sqlite3_realloc(void*, int);
SQLITE_API void sqlite3_free(void*);

/*
** CAPI3REF: Memory Allocator Statistics
**
** SQLite provides these two interfaces for reporting on the status
** of the [sqlite3_malloc()], [sqlite3_free()], and [sqlite3_realloc()]
** routines, which form the built-in memory allocation subsystem.
**
** ^The [sqlite3_memory_used()] routine returns the number of bytes
** of memory currently outstanding (malloced but not freed).
** ^The [sqlite3_memory_highwater()] routine returns the maximum
** value of [sqlite3_memory_used()] since the high-water mark
** was last reset.  ^The values returned by [sqlite3_memory_used()] and
** [sqlite3_memory_highwater()] include any overhead
** added by SQLite in its implementation of [sqlite3_malloc()],
** but not overhead added by the any underlying system library
** routines that [sqlite3_malloc()] may call.
**
** ^The memory high-water mark is reset to the current value of
** [sqlite3_memory_used()] if and only if the parameter to
** [sqlite3_memory_highwater()] is true.  ^The value returned
** by [sqlite3_memory_highwater(1)] is the high-water mark
** prior to the reset.
*/
SQLITE_API sqlite3_int64 sqlite3_memory_used(void);
SQLITE_API sqlite3_int64 sqlite3_memory_highwater(int resetFlag);

/*
** CAPI3REF: Pseudo-Random Number Generator
**
** SQLite contains a high-quality pseudo-random number generator (PRNG) used to
** select random [ROWID | ROWIDs] when inserting new records into a table that
** already uses the largest possible [ROWID].  The PRNG is also used for
** the build-in random() and randomblob() SQL functions.  This interface allows
** applications to access the same PRNG for other purposes.
**
** ^A call to this routine stores N bytes of randomness into buffer P.
**
** ^The first time this routine is invoked (either internally or by
** the application) the PRNG is seeded using randomness obtained
** from the xRandomness method of the default [sqlite3_vfs] object.
** ^On all subsequent invocations, the pseudo-randomness is generated
** internally and without recourse to the [sqlite3_vfs] xRandomness
** method.
*/
SQLITE_API void sqlite3_randomness(int N, void *P);

/*
** CAPI3REF: Compile-Time Authorization Callbacks
**
** ^This routine registers an authorizer callback with a particular
** [database connection], supplied in the first argument.
** ^The authorizer callback is invoked as SQL statements are being compiled
** by [sqlite3_prepare()] or its variants [sqlite3_prepare_v2()],
** [sqlite3_prepare16()] and [sqlite3_prepare16_v2()].  ^At various
** points during the compilation process, as logic is being created
** to perform various actions, the authorizer callback is invoked to
** see if those actions are allowed.  ^The authorizer callback should
** return [SQLITE_OK] to allow the action, [SQLITE_IGNORE] to disallow the
** specific action but allow the SQL statement to continue to be
** compiled, or [SQLITE_DENY] to cause the entire SQL statement to be
** rejected with an error.  ^If the authorizer callback returns
** any value other than [SQLITE_IGNORE], [SQLITE_OK], or [SQLITE_DENY]
** then the [sqlite3_prepare_v2()] or equivalent call that triggered
** the authorizer will fail with an error message.
**
** When the callback returns [SQLITE_OK], that means the operation
** requested is ok.  ^When the callback returns [SQLITE_DENY], the
** [sqlite3_prepare_v2()] or equivalent call that triggered the
** authorizer will fail with an error message explaining that
** access is denied. 
**
** ^The first parameter to the authorizer callback is a copy of the third
** parameter to the sqlite3_set_authorizer() interface. ^The second parameter
** to the callback is an integer [SQLITE_COPY | action code] that specifies
** the particular action to be authorized. ^The third through sixth parameters
** to the callback are zero-terminated strings that contain additional
** details about the action to be authorized.
**
** ^If the action code is [SQLITE_READ]
** and the callback returns [SQLITE_IGNORE] then the
** [prepared statement] statement is constructed to substitute
** a NULL value in place of the table column that would have
** been read if [SQLITE_OK] had been returned.  The [SQLITE_IGNORE]
** return can be used to deny an untrusted user access to individual
** columns of a table.
** ^If the action code is [SQLITE_DELETE] and the callback returns
** [SQLITE_IGNORE] then the [DELETE] operation proceeds but the
** [truncate optimization] is disabled and all rows are deleted individually.
**
** An authorizer is used when [sqlite3_prepare | preparing]
** SQL statements from an untrusted source, to ensure that the SQL statements
** do not try to access data they are not allowed to see, or that they do not
** try to execute malicious statements that damage the database.  For
** example, an application may allow a user to enter arbitrary
** SQL queries for evaluation by a database.  But the application does
** not want the user to be able to make arbitrary changes to the
** database.  An authorizer could then be put in place while the
** user-entered SQL is being [sqlite3_prepare | prepared] that
** disallows everything except [SELECT] statements.
**
** Applications that need to process SQL from untrusted sources
** might also consider lowering resource limits using [sqlite3_limit()]
** and limiting database size using the [max_page_count] [PRAGMA]
** in addition to using an authorizer.
**
** ^(Only a single authorizer can be in place on a database connection
** at a time.  Each call to sqlite3_set_authorizer overrides the
** previous call.)^  ^Disable the authorizer by installing a NULL callback.
** The authorizer is disabled by default.
**
** The authorizer callback must not do anything that will modify
** the database connection that invoked the authorizer callback.
** Note that [sqlite3_prepare_v2()] and [sqlite3_step()] both modify their
** database connections for the meaning of "modify" in this paragraph.
**
** ^When [sqlite3_prepare_v2()] is used to prepare a statement, the
** statement might be re-prepared during [sqlite3_step()] due to a 
** schema change.  Hence, the application should ensure that the
** correct authorizer callback remains in place during the [sqlite3_step()].
**
** ^Note that the authorizer callback is invoked only during
** [sqlite3_prepare()] or its variants.  Authorization is not
** performed during statement evaluation in [sqlite3_step()], unless
** as stated in the previous paragraph, sqlite3_step() invokes
** sqlite3_prepare_v2() to reprepare a statement after a schema change.
*/
SQLITE_API int sqlite3_set_authorizer(
  sqlite3*,
  int (*xAuth)(void*,int,const char*,const char*,const char*,const char*),
  void *pUserData
);

/*
** CAPI3REF: Authorizer Return Codes
**
** The [sqlite3_set_authorizer | authorizer callback function] must
** return either [SQLITE_OK] or one of these two constants in order
** to signal SQLite whether or not the action is permitted.  See the
** [sqlite3_set_authorizer | authorizer documentation] for additional
** information.
*/
#define SQLITE_DENY   1   /* Abort the SQL statement with an error */
#define SQLITE_IGNORE 2   /* Don't allow access, but don't generate an error */

/*
** CAPI3REF: Authorizer Action Codes
**
** The [sqlite3_set_authorizer()] interface registers a callback function
** that is invoked to authorize certain SQL statement actions.  The
** second parameter to the callback is an integer code that specifies
** what action is being authorized.  These are the integer action codes that
** the authorizer callback may be passed.
**
** These action code values signify what kind of operation is to be
** authorized.  The 3rd and 4th parameters to the authorization
** callback function will be parameters or NULL depending on which of these
** codes is used as the second parameter.  ^(The 5th parameter to the
** authorizer callback is the name of the database ("main", "temp",
** etc.) if applicable.)^  ^The 6th parameter to the authorizer callback
** is the name of the inner-most trigger or view that is responsible for
** the access attempt or NULL if this access attempt is directly from
** top-level SQL code.
*/
/******************************************* 3rd ************ 4th ***********/
#define SQLITE_CREATE_INDEX          1   /* Index Name      Table Name      */
#define SQLITE_CREATE_TABLE          2   /* Table Name      NULL            */
#define SQLITE_CREATE_TEMP_INDEX     3   /* Index Name      Table Name      */
#define SQLITE_CREATE_TEMP_TABLE     4   /* Table Name      NULL            */
#define SQLITE_CREATE_TEMP_TRIGGER   5   /* Trigger Name    Table Name      */
#define SQLITE_CREATE_TEMP_VIEW      6   /* View Name       NULL            */
#define SQLITE_CREATE_TRIGGER        7   /* Trigger Name    Table Name      */
#define SQLITE_CREATE_VIEW           8   /* View Name       NULL            */
#define SQLITE_DELETE                9   /* Table Name      NULL            */
#define SQLITE_DROP_INDEX           10   /* Index Name      Table Name      */
#define SQLITE_DROP_TABLE           11   /* Table Name      NULL            */
#define SQLITE_DROP_TEMP_INDEX      12   /* Index Name      Table Name      */
#define SQLITE_DROP_TEMP_TABLE      13   /* Table Name      NULL            */
#define SQLITE_DROP_TEMP_TRIGGER    14   /* Trigger Name    Table Name      */
#define SQLITE_DROP_TEMP_VIEW       15   /* View Name       NULL            */
#define SQLITE_DROP_TRIGGER         16   /* Trigger Name    Table Name      */
#define SQLITE_DROP_VIEW            17   /* View Name       NULL            */
#define SQLITE_INSERT               18   /* Table Name      NULL            */
#define SQLITE_PRAGMA               19   /* Pragma Name     1st arg or NULL */
#define SQLITE_READ                 20   /* Table Name      Column Name     */
#define SQLITE_SELECT               21   /* NULL            NULL            */
#define SQLITE_TRANSACTION          22   /* Operation       NULL            */
#define SQLITE_UPDATE               23   /* Table Name      Column Name     */
#define SQLITE_ATTACH               24   /* Filename        NULL            */
#define SQLITE_DETACH               25   /* Database Name   NULL            */
#define SQLITE_ALTER_TABLE          26   /* Database Name   Table Name      */
#define SQLITE_REINDEX              27   /* Index Name      NULL            */
#define SQLITE_ANALYZE              28   /* Table Name      NULL            */
#define SQLITE_CREATE_VTABLE        29   /* Table Name      Module Name     */
#define SQLITE_DROP_VTABLE          30   /* Table Name      Module Name     */
#define SQLITE_FUNCTION             31   /* NULL            Function Name   */
#define SQLITE_SAVEPOINT            32   /* Operation       Savepoint Name  */
#define SQLITE_COPY                  0   /* No longer used */

/*
** CAPI3REF: Tracing And Profiling Functions
**
** These routines register callback functions that can be used for
** tracing and profiling the execution of SQL statements.
**
** ^The callback function registered by sqlite3_trace() is invoked at
** various times when an SQL statement is being run by [sqlite3_step()].
** ^The sqlite3_trace() callback is invoked with a UTF-8 rendering of the
** SQL statement text as the statement first begins executing.
** ^(Additional sqlite3_trace() callbacks might occur
** as each triggered subprogram is entered.  The callbacks for triggers
** contain a UTF-8 SQL comment that identifies the trigger.)^
**
** ^The callback function registered by sqlite3_profile() is invoked
** as each SQL statement finishes.  ^The profile callback contains
** the original statement text and an estimate of wall-clock time
** of how long that statement took to run.  ^The profile callback
** time is in units of nanoseconds, however the current implementation
** is only capable of millisecond resolution so the six least significant
** digits in the time are meaningless.  Future versions of SQLite
** might provide greater resolution on the profiler callback.  The
** sqlite3_profile() function is considered experimental and is
** subject to change in future versions of SQLite.
*/
SQLITE_API void *sqlite3_trace(sqlite3*, void(*xTrace)(void*,const char*), void*);
SQLITE_API SQLITE_EXPERIMENTAL void *sqlite3_profile(sqlite3*,
   void(*xProfile)(void*,const char*,sqlite3_uint64), void*);

/*
** CAPI3REF: Query Progress Callbacks
**
** ^The sqlite3_progress_handler(D,N,X,P) interface causes the callback
** function X to be invoked periodically during long running calls to
** [sqlite3_exec()], [sqlite3_step()] and [sqlite3_get_table()] for
** database connection D.  An example use for this
** interface is to keep a GUI updated during a large query.
**
** ^The parameter P is passed through as the only parameter to the 
** callback function X.  ^The parameter N is the number of 
** [virtual machine instructions] that are evaluated between successive
** invocations of the callback X.
**
** ^Only a single progress handler may be defined at one time per
** [database connection]; setting a new progress handler cancels the
** old one.  ^Setting parameter X to NULL disables the progress handler.
** ^The progress handler is also disabled by setting N to a value less
** than 1.
**
** ^If the progress callback returns non-zero, the operation is
** interrupted.  This feature can be used to implement a
** "Cancel" button on a GUI progress dialog box.
**
** The progress handler callback must not do anything that will modify
** the database connection that invoked the progress handler.
** Note that [sqlite3_prepare_v2()] and [sqlite3_step()] both modify their
** database connections for the meaning of "modify" in this paragraph.
**
*/
SQLITE_API void sqlite3_progress_handler(sqlite3*, int, int(*)(void*), void*);

/*
** CAPI3REF: Opening A New Database Connection
**
** ^These routines open an SQLite database file whose name is given by the
** filename argument. ^The filename argument is interpreted as UTF-8 for
** sqlite3_open() and sqlite3_open_v2() and as UTF-16 in the native byte
** order for sqlite3_open16(). ^(A [database connection] handle is usually
** returned in *ppDb, even if an error occurs.  The only exception is that
** if SQLite is unable to allocate memory to hold the [sqlite3] object,
** a NULL will be written into *ppDb instead of a pointer to the [sqlite3]
** object.)^ ^(If the database is opened (and/or created) successfully, then
** [SQLITE_OK] is returned.  Otherwise an [error code] is returned.)^ ^The
** [sqlite3_errmsg()] or [sqlite3_errmsg16()] routines can be used to obtain
** an English language description of the error following a failure of any
** of the sqlite3_open() routines.
**
** ^The default encoding for the database will be UTF-8 if
** sqlite3_open() or sqlite3_open_v2() is called and
** UTF-16 in the native byte order if sqlite3_open16() is used.
**
** Whether or not an error occurs when it is opened, resources
** associated with the [database connection] handle should be released by
** passing it to [sqlite3_close()] when it is no longer required.
**
** The sqlite3_open_v2() interface works like sqlite3_open()
** except that it accepts two additional parameters for additional control
** over the new database connection.  ^(The flags parameter to
** sqlite3_open_v2() can take one of
** the following three values, optionally combined with the 
** [SQLITE_OPEN_NOMUTEX], [SQLITE_OPEN_FULLMUTEX], [SQLITE_OPEN_SHAREDCACHE],
** and/or [SQLITE_OPEN_PRIVATECACHE] flags:)^
**
** <dl>
** ^(<dt>[SQLITE_OPEN_READONLY]</dt>
** <dd>The database is opened in read-only mode.  If the database does not
** already exist, an error is returned.</dd>)^
**
** ^(<dt>[SQLITE_OPEN_READWRITE]</dt>
** <dd>The database is opened for reading and writing if possible, or reading
** only if the file is write protected by the operating system.  In either
** case the database must already exist, otherwise an error is returned.</dd>)^
**
** ^(<dt>[SQLITE_OPEN_READWRITE] | [SQLITE_OPEN_CREATE]</dt>
** <dd>The database is opened for reading and writing, and is created if
** it does not already exist. This is the behavior that is always used for
** sqlite3_open() and sqlite3_open16().</dd>)^
** </dl>
**
** If the 3rd parameter to sqlite3_open_v2() is not one of the
** combinations shown above or one of the combinations shown above combined
** with the [SQLITE_OPEN_NOMUTEX], [SQLITE_OPEN_FULLMUTEX],
** [SQLITE_OPEN_SHAREDCACHE] and/or [SQLITE_OPEN_PRIVATECACHE] flags,
** then the behavior is undefined.
**
** ^If the [SQLITE_OPEN_NOMUTEX] flag is set, then the database connection
** opens in the multi-thread [threading mode] as long as the single-thread
** mode has not been set at compile-time or start-time.  ^If the
** [SQLITE_OPEN_FULLMUTEX] flag is set then the database connection opens
** in the serialized [threading mode] unless single-thread was
** previously selected at compile-time or start-time.
** ^The [SQLITE_OPEN_SHAREDCACHE] flag causes the database connection to be
** eligible to use [shared cache mode], regardless of whether or not shared
** cache is enabled using [sqlite3_enable_shared_cache()].  ^The
** [SQLITE_OPEN_PRIVATECACHE] flag causes the database connection to not
** participate in [shared cache mode] even if it is enabled.
**
** ^If the filename is ":memory:", then a private, temporary in-memory database
** is created for the connection.  ^This in-memory database will vanish when
** the database connection is closed.  Future versions of SQLite might
** make use of additional special filenames that begin with the ":" character.
** It is recommended that when a database filename actually does begin with
** a ":" character you should prefix the filename with a pathname such as
** "./" to avoid ambiguity.
**
** ^If the filename is an empty string, then a private, temporary
** on-disk database will be created.  ^This private database will be
** automatically deleted as soon as the database connection is closed.
**
** ^The fourth parameter to sqlite3_open_v2() is the name of the
** [sqlite3_vfs] object that defines the operating system interface that
** the new database connection should use.  ^If the fourth parameter is
** a NULL pointer then the default [sqlite3_vfs] object is used.
**
** <b>Note to Windows users:</b>  The encoding used for the filename argument
** of sqlite3_open() and sqlite3_open_v2() must be UTF-8, not whatever
** codepage is currently defined.  Filenames containing international
** characters must be converted to UTF-8 prior to passing them into
** sqlite3_open() or sqlite3_open_v2().
*/
SQLITE_API int sqlite3_open(
  const char *filename,   /* Database filename (UTF-8) */
  sqlite3 **ppDb          /* OUT: SQLite db handle */
);
SQLITE_API int sqlite3_open16(
  const void *filename,   /* Database filename (UTF-16) */
  sqlite3 **ppDb          /* OUT: SQLite db handle */
);
SQLITE_API int sqlite3_open_v2(
  const char *filename,   /* Database filename (UTF-8) */
  sqlite3 **ppDb,         /* OUT: SQLite db handle */
  int flags,              /* Flags */
  const char *zVfs        /* Name of VFS module to use */
);

/*
** CAPI3REF: Error Codes And Messages
**
** ^The sqlite3_errcode() interface returns the numeric [result code] or
** [extended result code] for the most recent failed sqlite3_* API call
** associated with a [database connection]. If a prior API call failed
** but the most recent API call succeeded, the return value from
** sqlite3_errcode() is undefined.  ^The sqlite3_extended_errcode()
** interface is the same except that it always returns the 
** [extended result code] even when extended result codes are
** disabled.
**
** ^The sqlite3_errmsg() and sqlite3_errmsg16() return English-language
** text that describes the error, as either UTF-8 or UTF-16 respectively.
** ^(Memory to hold the error message string is managed internally.
** The application does not need to worry about freeing the result.
** However, the error string might be overwritten or deallocated by
** subsequent calls to other SQLite interface functions.)^
**
** When the serialized [threading mode] is in use, it might be the
** case that a second error occurs on a separate thread in between
** the time of the first error and the call to these interfaces.
** When that happens, the second error will be reported since these
** interfaces always report the most recent result.  To avoid
** this, each thread can obtain exclusive use of the [database connection] D
** by invoking [sqlite3_mutex_enter]([sqlite3_db_mutex](D)) before beginning
** to use D and invoking [sqlite3_mutex_leave]([sqlite3_db_mutex](D)) after
** all calls to the interfaces listed here are completed.
**
** If an interface fails with SQLITE_MISUSE, that means the interface
** was invoked incorrectly by the application.  In that case, the
** error code and message may or may not be set.
*/
SQLITE_API int sqlite3_errcode(sqlite3 *db);
SQLITE_API int sqlite3_extended_errcode(sqlite3 *db);
SQLITE_API const char *sqlite3_errmsg(sqlite3*);
SQLITE_API const void *sqlite3_errmsg16(sqlite3*);

/*
** CAPI3REF: SQL Statement Object
** KEYWORDS: {prepared statement} {prepared statements}
**
** An instance of this object represents a single SQL statement.
** This object is variously known as a "prepared statement" or a
** "compiled SQL statement" or simply as a "statement".
**
** The life of a statement object goes something like this:
**
** <ol>
** <li> Create the object using [sqlite3_prepare_v2()] or a related
**      function.
** <li> Bind values to [host parameters] using the sqlite3_bind_*()
**      interfaces.
** <li> Run the SQL by calling [sqlite3_step()] one or more times.
** <li> Reset the statement using [sqlite3_reset()] then go back
**      to step 2.  Do this zero or more times.
** <li> Destroy the object using [sqlite3_finalize()].
** </ol>
**
** Refer to documentation on individual methods above for additional
** information.
*/
typedef struct sqlite3_stmt sqlite3_stmt;

/*
** CAPI3REF: Run-time Limits
**
** ^(This interface allows the size of various constructs to be limited
** on a connection by connection basis.  The first parameter is the
** [database connection] whose limit is to be set or queried.  The
** second parameter is one of the [limit categories] that define a
** class of constructs to be size limited.  The third parameter is the
** new limit for that construct.)^
**
** ^If the new limit is a negative number, the limit is unchanged.
** ^(For each limit category SQLITE_LIMIT_<i>NAME</i> there is a 
** [limits | hard upper bound]
** set at compile-time by a C preprocessor macro called
** [limits | SQLITE_MAX_<i>NAME</i>].
** (The "_LIMIT_" in the name is changed to "_MAX_".))^
** ^Attempts to increase a limit above its hard upper bound are
** silently truncated to the hard upper bound.
**
** ^Regardless of whether or not the limit was changed, the 
** [sqlite3_limit()] interface returns the prior value of the limit.
** ^Hence, to find the current value of a limit without changing it,
** simply invoke this interface with the third parameter set to -1.
**
** Run-time limits are intended for use in applications that manage
** both their own internal database and also databases that are controlled
** by untrusted external sources.  An example application might be a
** web browser that has its own databases for storing history and
** separate databases controlled by JavaScript applications downloaded
** off the Internet.  The internal databases can be given the
** large, default limits.  Databases managed by external sources can
** be given much smaller limits designed to prevent a denial of service
** attack.  Developers might also want to use the [sqlite3_set_authorizer()]
** interface to further control untrusted SQL.  The size of the database
** created by an untrusted script can be contained using the
** [max_page_count] [PRAGMA].
**
** New run-time limit categories may be added in future releases.
*/
SQLITE_API int sqlite3_limit(sqlite3*, int id, int newVal);

/*
** CAPI3REF: Run-Time Limit Categories
** KEYWORDS: {limit category} {*limit categories}
**
** These constants define various performance limits
** that can be lowered at run-time using [sqlite3_limit()].
** The synopsis of the meanings of the various limits is shown below.
** Additional information is available at [limits | Limits in SQLite].
**
** <dl>
** ^(<dt>SQLITE_LIMIT_LENGTH</dt>
** <dd>The maximum size of any string or BLOB or table row, in bytes.<dd>)^
**
** ^(<dt>SQLITE_LIMIT_SQL_LENGTH</dt>
** <dd>The maximum length of an SQL statement, in bytes.</dd>)^
**
** ^(<dt>SQLITE_LIMIT_COLUMN</dt>
** <dd>The maximum number of columns in a table definition or in the
** result set of a [SELECT] or the maximum number of columns in an index
** or in an ORDER BY or GROUP BY clause.</dd>)^
**
** ^(<dt>SQLITE_LIMIT_EXPR_DEPTH</dt>
** <dd>The maximum depth of the parse tree on any expression.</dd>)^
**
** ^(<dt>SQLITE_LIMIT_COMPOUND_SELECT</dt>
** <dd>The maximum number of terms in a compound SELECT statement.</dd>)^
**
** ^(<dt>SQLITE_LIMIT_VDBE_OP</dt>
** <dd>The maximum number of instructions in a virtual machine program
** used to implement an SQL statement.  This limit is not currently
** enforced, though that might be added in some future release of
** SQLite.</dd>)^
**
** ^(<dt>SQLITE_LIMIT_FUNCTION_ARG</dt>
** <dd>The maximum number of arguments on a function.</dd>)^
**
** ^(<dt>SQLITE_LIMIT_ATTACHED</dt>
** <dd>The maximum number of [ATTACH | attached databases].)^</dd>
**
** ^(<dt>SQLITE_LIMIT_LIKE_PATTERN_LENGTH</dt>
** <dd>The maximum length of the pattern argument to the [LIKE] or
** [GLOB] operators.</dd>)^
**
** ^(<dt>SQLITE_LIMIT_VARIABLE_NUMBER</dt>
** <dd>The maximum index number of any [parameter] in an SQL statement.)^
**
** ^(<dt>SQLITE_LIMIT_TRIGGER_DEPTH</dt>
** <dd>The maximum depth of recursion for triggers.</dd>)^
** </dl>
*/
#define SQLITE_LIMIT_LENGTH                    0
#define SQLITE_LIMIT_SQL_LENGTH                1
#define SQLITE_LIMIT_COLUMN                    2
#define SQLITE_LIMIT_EXPR_DEPTH                3
#define SQLITE_LIMIT_COMPOUND_SELECT           4
#define SQLITE_LIMIT_VDBE_OP                   5
#define SQLITE_LIMIT_FUNCTION_ARG              6
#define SQLITE_LIMIT_ATTACHED                  7
#define SQLITE_LIMIT_LIKE_PATTERN_LENGTH       8
#define SQLITE_LIMIT_VARIABLE_NUMBER           9
#define SQLITE_LIMIT_TRIGGER_DEPTH            10

/*
** CAPI3REF: Compiling An SQL Statement
** KEYWORDS: {SQL statement compiler}
**
** To execute an SQL query, it must first be compiled into a byte-code
** program using one of these routines.
**
** The first argument, "db", is a [database connection] obtained from a
** prior successful call to [sqlite3_open()], [sqlite3_open_v2()] or
** [sqlite3_open16()].  The database connection must not have been closed.
**
** The second argument, "zSql", is the statement to be compiled, encoded
** as either UTF-8 or UTF-16.  The sqlite3_prepare() and sqlite3_prepare_v2()
** interfaces use UTF-8, and sqlite3_prepare16() and sqlite3_prepare16_v2()
** use UTF-16.
**
** ^If the nByte argument is less than zero, then zSql is read up to the
** first zero terminator. ^If nByte is non-negative, then it is the maximum
** number of  bytes read from zSql.  ^When nByte is non-negative, the
** zSql string ends at either the first '\000' or '\u0000' character or
** the nByte-th byte, whichever comes first. If the caller knows
** that the supplied string is nul-terminated, then there is a small
** performance advantage to be gained by passing an nByte parameter that
** is equal to the number of bytes in the input string <i>including</i>
** the nul-terminator bytes.
**
** ^If pzTail is not NULL then *pzTail is made to point to the first byte
** past the end of the first SQL statement in zSql.  These routines only
** compile the first statement in zSql, so *pzTail is left pointing to
** what remains uncompiled.
**
** ^*ppStmt is left pointing to a compiled [prepared statement] that can be
** executed using [sqlite3_step()].  ^If there is an error, *ppStmt is set
** to NULL.  ^If the input text contains no SQL (if the input is an empty
** string or a comment) then *ppStmt is set to NULL.
** The calling procedure is responsible for deleting the compiled
** SQL statement using [sqlite3_finalize()] after it has finished with it.
** ppStmt may not be NULL.
**
** ^On success, the sqlite3_prepare() family of routines return [SQLITE_OK];
** otherwise an [error code] is returned.
**
** The sqlite3_prepare_v2() and sqlite3_prepare16_v2() interfaces are
** recommended for all new programs. The two older interfaces are retained
** for backwards compatibility, but their use is discouraged.
** ^In the "v2" interfaces, the prepared statement
** that is returned (the [sqlite3_stmt] object) contains a copy of the
** original SQL text. This causes the [sqlite3_step()] interface to
** behave differently in three ways:
**
** <ol>
** <li>
** ^If the database schema changes, instead of returning [SQLITE_SCHEMA] as it
** always used to do, [sqlite3_step()] will automatically recompile the SQL
** statement and try to run it again.
** </li>
**
** <li>
** ^When an error occurs, [sqlite3_step()] will return one of the detailed
** [error codes] or [extended error codes].  ^The legacy behavior was that
** [sqlite3_step()] would only return a generic [SQLITE_ERROR] result code
** and the application would have to make a second call to [sqlite3_reset()]
** in order to find the underlying cause of the problem. With the "v2" prepare
** interfaces, the underlying reason for the error is returned immediately.
** </li>
**
** <li>
** ^If the specific value bound to [parameter | host parameter] in the 
** WHERE clause might influence the choice of query plan for a statement,
** then the statement will be automatically recompiled, as if there had been 
** a schema change, on the first  [sqlite3_step()] call following any change
** to the [sqlite3_bind_text | bindings] of that [parameter]. 
** ^The specific value of WHERE-clause [parameter] might influence the 
** choice of query plan if the parameter is the left-hand side of a [LIKE]
** or [GLOB] operator or if the parameter is compared to an indexed column
** and the [SQLITE_ENABLE_STAT2] compile-time option is enabled.
** the 
** </li>
** </ol>
*/
SQLITE_API int sqlite3_prepare(
  sqlite3 *db,            /* Database handle */
  const char *zSql,       /* SQL statement, UTF-8 encoded */
  int nByte,              /* Maximum length of zSql in bytes. */
  sqlite3_stmt **ppStmt,  /* OUT: Statement handle */
  const char **pzTail     /* OUT: Pointer to unused portion of zSql */
);
SQLITE_API int sqlite3_prepare_v2(
  sqlite3 *db,            /* Database handle */
  const char *zSql,       /* SQL statement, UTF-8 encoded */
  int nByte,              /* Maximum length of zSql in bytes. */
  sqlite3_stmt **ppStmt,  /* OUT: Statement handle */
  const char **pzTail     /* OUT: Pointer to unused portion of zSql */
);
SQLITE_API int sqlite3_prepare16(
  sqlite3 *db,            /* Database handle */
  const void *zSql,       /* SQL statement, UTF-16 encoded */
  int nByte,              /* Maximum length of zSql in bytes. */
  sqlite3_stmt **ppStmt,  /* OUT: Statement handle */
  const void **pzTail     /* OUT: Pointer to unused portion of zSql */
);
SQLITE_API int sqlite3_prepare16_v2(
  sqlite3 *db,            /* Database handle */
  const void *zSql,       /* SQL statement, UTF-16 encoded */
  int nByte,              /* Maximum length of zSql in bytes. */
  sqlite3_stmt **ppStmt,  /* OUT: Statement handle */
  const void **pzTail     /* OUT: Pointer to unused portion of zSql */
);

/*
** CAPI3REF: Retrieving Statement SQL
**
** ^This interface can be used to retrieve a saved copy of the original
** SQL text used to create a [prepared statement] if that statement was
** compiled using either [sqlite3_prepare_v2()] or [sqlite3_prepare16_v2()].
*/
SQLITE_API const char *sqlite3_sql(sqlite3_stmt *pStmt);

/*
** CAPI3REF: Determine If An SQL Statement Writes The Database
**
** ^The sqlite3_stmt_readonly(X) interface returns true (non-zero) if
** and only if the [prepared statement] X makes no direct changes to
** the content of the database file.
**
** Note that [application-defined SQL functions] or
** [virtual tables] might change the database indirectly as a side effect.  
** ^(For example, if an application defines a function "eval()" that 
** calls [sqlite3_exec()], then the following SQL statement would
** change the database file through side-effects:
**
** <blockquote><pre>
**    SELECT eval('DELETE FROM t1') FROM t2;
** </pre></blockquote>
**
** But because the [SELECT] statement does not change the database file
** directly, sqlite3_stmt_readonly() would still return true.)^
**
** ^Transaction control statements such as [BEGIN], [COMMIT], [ROLLBACK],
** [SAVEPOINT], and [RELEASE] cause sqlite3_stmt_readonly() to return true,
** since the statements themselves do not actually modify the database but
** rather they control the timing of when other statements modify the 
** database.  ^The [ATTACH] and [DETACH] statements also cause
** sqlite3_stmt_readonly() to return true since, while those statements
** change the configuration of a database connection, they do not make 
** changes to the content of the database files on disk.
*/
SQLITE_API int sqlite3_stmt_readonly(sqlite3_stmt *pStmt);

/*
** CAPI3REF: Dynamically Typed Value Object
** KEYWORDS: {protected sqlite3_value} {unprotected sqlite3_value}
**
** SQLite uses the sqlite3_value object to represent all values
** that can be stored in a database table. SQLite uses dynamic typing
** for the values it stores.  ^Values stored in sqlite3_value objects
** can be integers, floating point values, strings, BLOBs, or NULL.
**
** An sqlite3_value object may be either "protected" or "unprotected".
** Some interfaces require a protected sqlite3_value.  Other interfaces
** will accept either a protected or an unprotected sqlite3_value.
** Every interface that accepts sqlite3_value arguments specifies
** whether or not it requires a protected sqlite3_value.
**
** The terms "protected" and "unprotected" refer to whether or not
** a mutex is held.  An internal mutex is held for a protected
** sqlite3_value object but no mutex is held for an unprotected
** sqlite3_value object.  If SQLite is compiled to be single-threaded
** (with [SQLITE_THREADSAFE=0] and with [sqlite3_threadsafe()] returning 0)
** or if SQLite is run in one of reduced mutex modes 
** [SQLITE_CONFIG_SINGLETHREAD] or [SQLITE_CONFIG_MULTITHREAD]
** then there is no distinction between protected and unprotected
** sqlite3_value objects and they can be used interchangeably.  However,
** for maximum code portability it is recommended that applications
** still make the distinction between protected and unprotected
** sqlite3_value objects even when not strictly required.
**
** ^The sqlite3_value objects that are passed as parameters into the
** implementation of [application-defined SQL functions] are protected.
** ^The sqlite3_value object returned by
** [sqlite3_column_value()] is unprotected.
** Unprotected sqlite3_value objects may only be used with
** [sqlite3_result_value()] and [sqlite3_bind_value()].
** The [sqlite3_value_blob | sqlite3_value_type()] family of
** interfaces require protected sqlite3_value objects.
*/
typedef struct Mem sqlite3_value;

/*
** CAPI3REF: SQL Function Context Object
**
** The context in which an SQL function executes is stored in an
** sqlite3_context object.  ^A pointer to an sqlite3_context object
** is always first parameter to [application-defined SQL functions].
** The application-defined SQL function implementation will pass this
** pointer through into calls to [sqlite3_result_int | sqlite3_result()],
** [sqlite3_aggregate_context()], [sqlite3_user_data()],
** [sqlite3_context_db_handle()], [sqlite3_get_auxdata()],
** and/or [sqlite3_set_auxdata()].
*/
typedef struct sqlite3_context sqlite3_context;

/*
** CAPI3REF: Binding Values To Prepared Statements
** KEYWORDS: {host parameter} {host parameters} {host parameter name}
** KEYWORDS: {SQL parameter} {SQL parameters} {parameter binding}
**
** ^(In the SQL statement text input to [sqlite3_prepare_v2()] and its variants,
** literals may be replaced by a [parameter] that matches one of following
** templates:
**
** <ul>
** <li>  ?
** <li>  ?NNN
** <li>  :VVV
** <li>  @VVV
** <li>  $VVV
** </ul>
**
** In the templates above, NNN represents an integer literal,
** and VVV represents an alphanumeric identifier.)^  ^The values of these
** parameters (also called "host parameter names" or "SQL parameters")
** can be set using the sqlite3_bind_*() routines defined here.
**
** ^The first argument to the sqlite3_bind_*() routines is always
** a pointer to the [sqlite3_stmt] object returned from
** [sqlite3_prepare_v2()] or its variants.
**
** ^The second argument is the index of the SQL parameter to be set.
** ^The leftmost SQL parameter has an index of 1.  ^When the same named
** SQL parameter is used more than once, second and subsequent
** occurrences have the same index as the first occurrence.
** ^The index for named parameters can be looked up using the
** [sqlite3_bind_parameter_index()] API if desired.  ^The index
** for "?NNN" parameters is the value of NNN.
** ^The NNN value must be between 1 and the [sqlite3_limit()]
** parameter [SQLITE_LIMIT_VARIABLE_NUMBER] (default value: 999).
**
** ^The third argument is the value to bind to the parameter.
**
** ^(In those routines that have a fourth argument, its value is the
** number of bytes in the parameter.  To be clear: the value is the
** number of <u>bytes</u> in the value, not the number of characters.)^
** ^If the fourth parameter is negative, the length of the string is
** the number of bytes up to the first zero terminator.
**
** ^The fifth argument to sqlite3_bind_blob(), sqlite3_bind_text(), and
** sqlite3_bind_text16() is a destructor used to dispose of the BLOB or
** string after SQLite has finished with it.  ^The destructor is called
** to dispose of the BLOB or string even if the call to sqlite3_bind_blob(),
** sqlite3_bind_text(), or sqlite3_bind_text16() fails.  
** ^If the fifth argument is
** the special value [SQLITE_STATIC], then SQLite assumes that the
** information is in static, unmanaged space and does not need to be freed.
** ^If the fifth argument has the value [SQLITE_TRANSIENT], then
** SQLite makes its own private copy of the data immediately, before
** the sqlite3_bind_*() routine returns.
**
** ^The sqlite3_bind_zeroblob() routine binds a BLOB of length N that
** is filled with zeroes.  ^A zeroblob uses a fixed amount of memory
** (just an integer to hold its size) while it is being processed.
** Zeroblobs are intended to serve as placeholders for BLOBs whose
** content is later written using
** [sqlite3_blob_open | incremental BLOB I/O] routines.
** ^A negative value for the zeroblob results in a zero-length BLOB.
**
** ^If any of the sqlite3_bind_*() routines are called with a NULL pointer
** for the [prepared statement] or with a prepared statement for which
** [sqlite3_step()] has been called more recently than [sqlite3_reset()],
** then the call will return [SQLITE_MISUSE].  If any sqlite3_bind_()
** routine is passed a [prepared statement] that has been finalized, the
** result is undefined and probably harmful.
**
** ^Bindings are not cleared by the [sqlite3_reset()] routine.
** ^Unbound parameters are interpreted as NULL.
**
** ^The sqlite3_bind_* routines return [SQLITE_OK] on success or an
** [error code] if anything goes wrong.
** ^[SQLITE_RANGE] is returned if the parameter
** index is out of range.  ^[SQLITE_NOMEM] is returned if malloc() fails.
**
** See also: [sqlite3_bind_parameter_count()],
** [sqlite3_bind_parameter_name()], and [sqlite3_bind_parameter_index()].
*/
SQLITE_API int sqlite3_bind_blob(sqlite3_stmt*, int, const void*, int n, void(*)(void*));
SQLITE_API int sqlite3_bind_double(sqlite3_stmt*, int, double);
SQLITE_API int sqlite3_bind_int(sqlite3_stmt*, int, int);
SQLITE_API int sqlite3_bind_int64(sqlite3_stmt*, int, sqlite3_int64);
SQLITE_API int sqlite3_bind_null(sqlite3_stmt*, int);
SQLITE_API int sqlite3_bind_text(sqlite3_stmt*, int, const char*, int n, void(*)(void*));
SQLITE_API int sqlite3_bind_text16(sqlite3_stmt*, int, const void*, int, void(*)(void*));
SQLITE_API int sqlite3_bind_value(sqlite3_stmt*, int, const sqlite3_value*);
SQLITE_API int sqlite3_bind_zeroblob(sqlite3_stmt*, int, int n);

/*
** CAPI3REF: Number Of SQL Parameters
**
** ^This routine can be used to find the number of [SQL parameters]
** in a [prepared statement].  SQL parameters are tokens of the
** form "?", "?NNN", ":AAA", "$AAA", or "@AAA" that serve as
** placeholders for values that are [sqlite3_bind_blob | bound]
** to the parameters at a later time.
**
** ^(This routine actually returns the index of the largest (rightmost)
** parameter. For all forms except ?NNN, this will correspond to the
** number of unique parameters.  If parameters of the ?NNN form are used,
** there may be gaps in the list.)^
**
** See also: [sqlite3_bind_blob|sqlite3_bind()],
** [sqlite3_bind_parameter_name()], and
** [sqlite3_bind_parameter_index()].
*/
SQLITE_API int sqlite3_bind_parameter_count(sqlite3_stmt*);

/*
** CAPI3REF: Name Of A Host Parameter
**
** ^The sqlite3_bind_parameter_name(P,N) interface returns
** the name of the N-th [SQL parameter] in the [prepared statement] P.
** ^(SQL parameters of the form "?NNN" or ":AAA" or "@AAA" or "$AAA"
** have a name which is the string "?NNN" or ":AAA" or "@AAA" or "$AAA"
** respectively.
** In other words, the initial ":" or "$" or "@" or "?"
** is included as part of the name.)^
** ^Parameters of the form "?" without a following integer have no name
** and are referred to as "nameless" or "anonymous parameters".
**
** ^The first host parameter has an index of 1, not 0.
**
** ^If the value N is out of range or if the N-th parameter is
** nameless, then NULL is returned.  ^The returned string is
** always in UTF-8 encoding even if the named parameter was
** originally specified as UTF-16 in [sqlite3_prepare16()] or
** [sqlite3_prepare16_v2()].
**
** See also: [sqlite3_bind_blob|sqlite3_bind()],
** [sqlite3_bind_parameter_count()], and
** [sqlite3_bind_parameter_index()].
*/
SQLITE_API const char *sqlite3_bind_parameter_name(sqlite3_stmt*, int);

/*
** CAPI3REF: Index Of A Parameter With A Given Name
**
** ^Return the index of an SQL parameter given its name.  ^The
** index value returned is suitable for use as the second
** parameter to [sqlite3_bind_blob|sqlite3_bind()].  ^A zero
** is returned if no matching parameter is found.  ^The parameter
** name must be given in UTF-8 even if the original statement
** was prepared from UTF-16 text using [sqlite3_prepare16_v2()].
**
** See also: [sqlite3_bind_blob|sqlite3_bind()],
** [sqlite3_bind_parameter_count()], and
** [sqlite3_bind_parameter_index()].
*/
SQLITE_API int sqlite3_bind_parameter_index(sqlite3_stmt*, const char *zName);

/*
** CAPI3REF: Reset All Bindings On A Prepared Statement
**
** ^Contrary to the intuition of many, [sqlite3_reset()] does not reset
** the [sqlite3_bind_blob | bindings] on a [prepared statement].
** ^Use this routine to reset all host parameters to NULL.
*/
SQLITE_API int sqlite3_clear_bindings(sqlite3_stmt*);

/*
** CAPI3REF: Number Of Columns In A Result Set
**
** ^Return the number of columns in the result set returned by the
** [prepared statement]. ^This routine returns 0 if pStmt is an SQL
** statement that does not return data (for example an [UPDATE]).
**
** See also: [sqlite3_data_count()]
*/
SQLITE_API int sqlite3_column_count(sqlite3_stmt *pStmt);

/*
** CAPI3REF: Column Names In A Result Set
**
** ^These routines return the name assigned to a particular column
** in the result set of a [SELECT] statement.  ^The sqlite3_column_name()
** interface returns a pointer to a zero-terminated UTF-8 string
** and sqlite3_column_name16() returns a pointer to a zero-terminated
** UTF-16 string.  ^The first parameter is the [prepared statement]
** that implements the [SELECT] statement. ^The second parameter is the
** column number.  ^The leftmost column is number 0.
**
** ^The returned string pointer is valid until either the [prepared statement]
** is destroyed by [sqlite3_finalize()] or until the statement is automatically
** reprepared by the first call to [sqlite3_step()] for a particular run
** or until the next call to
** sqlite3_column_name() or sqlite3_column_name16() on the same column.
**
** ^If sqlite3_malloc() fails during the processing of either routine
** (for example during a conversion from UTF-8 to UTF-16) then a
** NULL pointer is returned.
**
** ^The name of a result column is the value of the "AS" clause for
** that column, if there is an AS clause.  If there is no AS clause
** then the name of the column is unspecified and may change from
** one release of SQLite to the next.
*/
SQLITE_API const char *sqlite3_column_name(sqlite3_stmt*, int N);
SQLITE_API const void *sqlite3_column_name16(sqlite3_stmt*, int N);

/*
** CAPI3REF: Source Of Data In A Query Result
**
** ^These routines provide a means to determine the database, table, and
** table column that is the origin of a particular result column in
** [SELECT] statement.
** ^The name of the database or table or column can be returned as
** either a UTF-8 or UTF-16 string.  ^The _database_ routines return
** the database name, the _table_ routines return the table name, and
** the origin_ routines return the column name.
** ^The returned string is valid until the [prepared statement] is destroyed
** using [sqlite3_finalize()] or until the statement is automatically
** reprepared by the first call to [sqlite3_step()] for a particular run
** or until the same information is requested
** again in a different encoding.
**
** ^The names returned are the original un-aliased names of the
** database, table, and column.
**
** ^The first argument to these interfaces is a [prepared statement].
** ^These functions return information about the Nth result column returned by
** the statement, where N is the second function argument.
** ^The left-most column is column 0 for these routines.
**
** ^If the Nth column returned by the statement is an expression or
** subquery and is not a column value, then all of these functions return
** NULL.  ^These routine might also return NULL if a memory allocation error
** occurs.  ^Otherwise, they return the name of the attached database, table,
** or column that query result column was extracted from.
**
** ^As with all other SQLite APIs, those whose names end with "16" return
** UTF-16 encoded strings and the other functions return UTF-8.
**
** ^These APIs are only available if the library was compiled with the
** [SQLITE_ENABLE_COLUMN_METADATA] C-preprocessor symbol.
**
** If two or more threads call one or more of these routines against the same
** prepared statement and column at the same time then the results are
** undefined.
**
** If two or more threads call one or more
** [sqlite3_column_database_name | column metadata interfaces]
** for the same [prepared statement] and result column
** at the same time then the results are undefined.
*/
SQLITE_API const char *sqlite3_column_database_name(sqlite3_stmt*,int);
SQLITE_API const void *sqlite3_column_database_name16(sqlite3_stmt*,int);
SQLITE_API const char *sqlite3_column_table_name(sqlite3_stmt*,int);
SQLITE_API const void *sqlite3_column_table_name16(sqlite3_stmt*,int);
SQLITE_API const char *sqlite3_column_origin_name(sqlite3_stmt*,int);
SQLITE_API const void *sqlite3_column_origin_name16(sqlite3_stmt*,int);

/*
** CAPI3REF: Declared Datatype Of A Query Result
**
** ^(The first parameter is a [prepared statement].
** If this statement is a [SELECT] statement and the Nth column of the
** returned result set of that [SELECT] is a table column (not an
** expression or subquery) then the declared type of the table
** column is returned.)^  ^If the Nth column of the result set is an
** expression or subquery, then a NULL pointer is returned.
** ^The returned string is always UTF-8 encoded.
**
** ^(For example, given the database schema:
**
** CREATE TABLE t1(c1 VARIANT);
**
** and the following statement to be compiled:
**
** SELECT c1 + 1, c1 FROM t1;
**
** this routine would return the string "VARIANT" for the second result
** column (i==1), and a NULL pointer for the first result column (i==0).)^
**
** ^SQLite uses dynamic run-time typing.  ^So just because a column
** is declared to contain a particular type does not mean that the
** data stored in that column is of the declared type.  SQLite is
** strongly typed, but the typing is dynamic not static.  ^Type
** is associated with individual values, not with the containers
** used to hold those values.
*/
SQLITE_API const char *sqlite3_column_decltype(sqlite3_stmt*,int);
SQLITE_API const void *sqlite3_column_decltype16(sqlite3_stmt*,int);

/*
** CAPI3REF: Evaluate An SQL Statement
**
** After a [prepared statement] has been prepared using either
** [sqlite3_prepare_v2()] or [sqlite3_prepare16_v2()] or one of the legacy
** interfaces [sqlite3_prepare()] or [sqlite3_prepare16()], this function
** must be called one or more times to evaluate the statement.
**
** The details of the behavior of the sqlite3_step() interface depend
** on whether the statement was prepared using the newer "v2" interface
** [sqlite3_prepare_v2()] and [sqlite3_prepare16_v2()] or the older legacy
** interface [sqlite3_prepare()] and [sqlite3_prepare16()].  The use of the
** new "v2" interface is recommended for new applications but the legacy
** interface will continue to be supported.
**
** ^In the legacy interface, the return value will be either [SQLITE_BUSY],
** [SQLITE_DONE], [SQLITE_ROW], [SQLITE_ERROR], or [SQLITE_MISUSE].
** ^With the "v2" interface, any of the other [result codes] or
** [extended result codes] might be returned as well.
**
** ^[SQLITE_BUSY] means that the database engine was unable to acquire the
** database locks it needs to do its job.  ^If the statement is a [COMMIT]
** or occurs outside of an explicit transaction, then you can retry the
** statement.  If the statement is not a [COMMIT] and occurs within a
** explicit transaction then you should rollback the transaction before
** continuing.
**
** ^[SQLITE_DONE] means that the statement has finished executing
** successfully.  sqlite3_step() should not be called again on this virtual
** machine without first calling [sqlite3_reset()] to reset the virtual
** machine back to its initial state.
**
** ^If the SQL statement being executed returns any data, then [SQLITE_ROW]
** is returned each time a new row of data is ready for processing by the
** caller. The values may be accessed using the [column access functions].
** sqlite3_step() is called again to retrieve the next row of data.
**
** ^[SQLITE_ERROR] means that a run-time error (such as a constraint
** violation) has occurred.  sqlite3_step() should not be called again on
** the VM. More information may be found by calling [sqlite3_errmsg()].
** ^With the legacy interface, a more specific error code (for example,
** [SQLITE_INTERRUPT], [SQLITE_SCHEMA], [SQLITE_CORRUPT], and so forth)
** can be obtained by calling [sqlite3_reset()] on the
** [prepared statement].  ^In the "v2" interface,
** the more specific error code is returned directly by sqlite3_step().
**
** [SQLITE_MISUSE] means that the this routine was called inappropriately.
** Perhaps it was called on a [prepared statement] that has
** already been [sqlite3_finalize | finalized] or on one that had
** previously returned [SQLITE_ERROR] or [SQLITE_DONE].  Or it could
** be the case that the same database connection is being used by two or
** more threads at the same moment in time.
**
** For all versions of SQLite up to and including 3.6.23.1, a call to
** [sqlite3_reset()] was required after sqlite3_step() returned anything
** other than [SQLITE_ROW] before any subsequent invocation of
** sqlite3_step().  Failure to reset the prepared statement using 
** [sqlite3_reset()] would result in an [SQLITE_MISUSE] return from
** sqlite3_step().  But after version 3.6.23.1, sqlite3_step() began
** calling [sqlite3_reset()] automatically in this circumstance rather
** than returning [SQLITE_MISUSE].  This is not considered a compatibility
** break because any application that ever receives an SQLITE_MISUSE error
** is broken by definition.  The [SQLITE_OMIT_AUTORESET] compile-time option
** can be used to restore the legacy behavior.
**
** <b>Goofy Interface Alert:</b> In the legacy interface, the sqlite3_step()
** API always returns a generic error code, [SQLITE_ERROR], following any
** error other than [SQLITE_BUSY] and [SQLITE_MISUSE].  You must call
** [sqlite3_reset()] or [sqlite3_finalize()] in order to find one of the
** specific [error codes] that better describes the error.
** We admit that this is a goofy design.  The problem has been fixed
** with the "v2" interface.  If you prepare all of your SQL statements
** using either [sqlite3_prepare_v2()] or [sqlite3_prepare16_v2()] instead
** of the legacy [sqlite3_prepare()] and [sqlite3_prepare16()] interfaces,
** then the more specific [error codes] are returned directly
** by sqlite3_step().  The use of the "v2" interface is recommended.
*/
SQLITE_API int sqlite3_step(sqlite3_stmt*);

/*
** CAPI3REF: Number of columns in a result set
**
** ^The sqlite3_data_count(P) interface returns the number of columns in the
** current row of the result set of [prepared statement] P.
** ^If prepared statement P does not have results ready to return
** (via calls to the [sqlite3_column_int | sqlite3_column_*()] of
** interfaces) then sqlite3_data_count(P) returns 0.
** ^The sqlite3_data_count(P) routine also returns 0 if P is a NULL pointer.
**
** See also: [sqlite3_column_count()]
*/
SQLITE_API int sqlite3_data_count(sqlite3_stmt *pStmt);

/*
** CAPI3REF: Fundamental Datatypes
** KEYWORDS: SQLITE_TEXT
**
** ^(Every value in SQLite has one of five fundamental datatypes:
**
** <ul>
** <li> 64-bit signed integer
** <li> 64-bit IEEE floating point number
** <li> string
** <li> BLOB
** <li> NULL
** </ul>)^
**
** These constants are codes for each of those types.
**
** Note that the SQLITE_TEXT constant was also used in SQLite version 2
** for a completely different meaning.  Software that links against both
** SQLite version 2 and SQLite version 3 should use SQLITE3_TEXT, not
** SQLITE_TEXT.
*/
#define SQLITE_INTEGER  1
#define SQLITE_FLOAT    2
#define SQLITE_BLOB     4
#define SQLITE_NULL     5
#ifdef SQLITE_TEXT
# undef SQLITE_TEXT
#else
# define SQLITE_TEXT     3
#endif
#define SQLITE3_TEXT     3

/*
** CAPI3REF: Result Values From A Query
** KEYWORDS: {column access functions}
**
** These routines form the "result set" interface.
**
** ^These routines return information about a single column of the current
** result row of a query.  ^In every case the first argument is a pointer
** to the [prepared statement] that is being evaluated (the [sqlite3_stmt*]
** that was returned from [sqlite3_prepare_v2()] or one of its variants)
** and the second argument is the index of the column for which information
** should be returned. ^The leftmost column of the result set has the index 0.
** ^The number of columns in the result can be determined using
** [sqlite3_column_count()].
**
** If the SQL statement does not currently point to a valid row, or if the
** column index is out of range, the result is undefined.
** These routines may only be called when the most recent call to
** [sqlite3_step()] has returned [SQLITE_ROW] and neither
** [sqlite3_reset()] nor [sqlite3_finalize()] have been called subsequently.
** If any of these routines are called after [sqlite3_reset()] or
** [sqlite3_finalize()] or after [sqlite3_step()] has returned
** something other than [SQLITE_ROW], the results are undefined.
** If [sqlite3_step()] or [sqlite3_reset()] or [sqlite3_finalize()]
** are called from a different thread while any of these routines
** are pending, then the results are undefined.
**
** ^The sqlite3_column_type() routine returns the
** [SQLITE_INTEGER | datatype code] for the initial data type
** of the result column.  ^The returned value is one of [SQLITE_INTEGER],
** [SQLITE_FLOAT], [SQLITE_TEXT], [SQLITE_BLOB], or [SQLITE_NULL].  The value
** returned by sqlite3_column_type() is only meaningful if no type
** conversions have occurred as described below.  After a type conversion,
** the value returned by sqlite3_column_type() is undefined.  Future
** versions of SQLite may change the behavior of sqlite3_column_type()
** following a type conversion.
**
** ^If the result is a BLOB or UTF-8 string then the sqlite3_column_bytes()
** routine returns the number of bytes in that BLOB or string.
** ^If the result is a UTF-16 string, then sqlite3_column_bytes() converts
** the string to UTF-8 and then returns the number of bytes.
** ^If the result is a numeric value then sqlite3_column_bytes() uses
** [sqlite3_snprintf()] to convert that value to a UTF-8 string and returns
** the number of bytes in that string.
** ^If the result is NULL, then sqlite3_column_bytes() returns zero.
**
** ^If the result is a BLOB or UTF-16 string then the sqlite3_column_bytes16()
** routine returns the number of bytes in that BLOB or string.
** ^If the result is a UTF-8 string, then sqlite3_column_bytes16() converts
** the string to UTF-16 and then returns the number of bytes.
** ^If the result is a numeric value then sqlite3_column_bytes16() uses
** [sqlite3_snprintf()] to convert that value to a UTF-16 string and returns
** the number of bytes in that string.
** ^If the result is NULL, then sqlite3_column_bytes16() returns zero.
**
** ^The values returned by [sqlite3_column_bytes()] and 
** [sqlite3_column_bytes16()] do not include the zero terminators at the end
** of the string.  ^For clarity: the values returned by
** [sqlite3_column_bytes()] and [sqlite3_column_bytes16()] are the number of
** bytes in the string, not the number of characters.
**
** ^Strings returned by sqlite3_column_text() and sqlite3_column_text16(),
** even empty strings, are always zero terminated.  ^The return
** value from sqlite3_column_blob() for a zero-length BLOB is a NULL pointer.
**
** ^The object returned by [sqlite3_column_value()] is an
** [unprotected sqlite3_value] object.  An unprotected sqlite3_value object
** may only be used with [sqlite3_bind_value()] and [sqlite3_result_value()].
** If the [unprotected sqlite3_value] object returned by
** [sqlite3_column_value()] is used in any other way, including calls
** to routines like [sqlite3_value_int()], [sqlite3_value_text()],
** or [sqlite3_value_bytes()], then the behavior is undefined.
**
** These routines attempt to convert the value where appropriate.  ^For
** example, if the internal representation is FLOAT and a text result
** is requested, [sqlite3_snprintf()] is used internally to perform the
** conversion automatically.  ^(The following table details the conversions
** that are applied:
**
** <blockquote>
** <table border="1">
** <tr><th> Internal<br>Type <th> Requested<br>Type <th>  Conversion
**
** <tr><td>  NULL    <td> INTEGER   <td> Result is 0
** <tr><td>  NULL    <td>  FLOAT    <td> Result is 0.0
** <tr><td>  NULL    <td>   TEXT    <td> Result is NULL pointer
** <tr><td>  NULL    <td>   BLOB    <td> Result is NULL pointer
** <tr><td> INTEGER  <td>  FLOAT    <td> Convert from integer to float
** <tr><td> INTEGER  <td>   TEXT    <td> ASCII rendering of the integer
** <tr><td> INTEGER  <td>   BLOB    <td> Same as INTEGER->TEXT
** <tr><td>  FLOAT   <td> INTEGER   <td> Convert from float to integer
** <tr><td>  FLOAT   <td>   TEXT    <td> ASCII rendering of the float
** <tr><td>  FLOAT   <td>   BLOB    <td> Same as FLOAT->TEXT
** <tr><td>  TEXT    <td> INTEGER   <td> Use atoi()
** <tr><td>  TEXT    <td>  FLOAT    <td> Use atof()
** <tr><td>  TEXT    <td>   BLOB    <td> No change
** <tr><td>  BLOB    <td> INTEGER   <td> Convert to TEXT then use atoi()
** <tr><td>  BLOB    <td>  FLOAT    <td> Convert to TEXT then use atof()
** <tr><td>  BLOB    <td>   TEXT    <td> Add a zero terminator if needed
** </table>
** </blockquote>)^
**
** The table above makes reference to standard C library functions atoi()
** and atof().  SQLite does not really use these functions.  It has its
** own equivalent internal routines.  The atoi() and atof() names are
** used in the table for brevity and because they are familiar to most
** C programmers.
**
** Note that when type conversions occur, pointers returned by prior
** calls to sqlite3_column_blob(), sqlite3_column_text(), and/or
** sqlite3_column_text16() may be invalidated.
** Type conversions and pointer invalidations might occur
** in the following cases:
**
** <ul>
** <li> The initial content is a BLOB and sqlite3_column_text() or
**      sqlite3_column_text16() is called.  A zero-terminator might
**      need to be added to the string.</li>
** <li> The initial content is UTF-8 text and sqlite3_column_bytes16() or
**      sqlite3_column_text16() is called.  The content must be converted
**      to UTF-16.</li>
** <li> The initial content is UTF-16 text and sqlite3_column_bytes() or
**      sqlite3_column_text() is called.  The content must be converted
**      to UTF-8.</li>
** </ul>
**
** ^Conversions between UTF-16be and UTF-16le are always done in place and do
** not invalidate a prior pointer, though of course the content of the buffer
** that the prior pointer references will have been modified.  Other kinds
** of conversion are done in place when it is possible, but sometimes they
** are not possible and in those cases prior pointers are invalidated.
**
** The safest and easiest to remember policy is to invoke these routines
** in one of the following ways:
**
** <ul>
**  <li>sqlite3_column_text() followed by sqlite3_column_bytes()</li>
**  <li>sqlite3_column_blob() followed by sqlite3_column_bytes()</li>
**  <li>sqlite3_column_text16() followed by sqlite3_column_bytes16()</li>
** </ul>
**
** In other words, you should call sqlite3_column_text(),
** sqlite3_column_blob(), or sqlite3_column_text16() first to force the result
** into the desired format, then invoke sqlite3_column_bytes() or
** sqlite3_column_bytes16() to find the size of the result.  Do not mix calls
** to sqlite3_column_text() or sqlite3_column_blob() with calls to
** sqlite3_column_bytes16(), and do not mix calls to sqlite3_column_text16()
** with calls to sqlite3_column_bytes().
**
** ^The pointers returned are valid until a type conversion occurs as
** described above, or until [sqlite3_step()] or [sqlite3_reset()] or
** [sqlite3_finalize()] is called.  ^The memory space used to hold strings
** and BLOBs is freed automatically.  Do <b>not</b> pass the pointers returned
** [sqlite3_column_blob()], [sqlite3_column_text()], etc. into
** [sqlite3_free()].
**
** ^(If a memory allocation error occurs during the evaluation of any
** of these routines, a default value is returned.  The default value
** is either the integer 0, the floating point number 0.0, or a NULL
** pointer.  Subsequent calls to [sqlite3_errcode()] will return
** [SQLITE_NOMEM].)^
*/
SQLITE_API const void *sqlite3_column_blob(sqlite3_stmt*, int iCol);
SQLITE_API int sqlite3_column_bytes(sqlite3_stmt*, int iCol);
SQLITE_API int sqlite3_column_bytes16(sqlite3_stmt*, int iCol);
SQLITE_API double sqlite3_column_double(sqlite3_stmt*, int iCol);
SQLITE_API int sqlite3_column_int(sqlite3_stmt*, int iCol);
SQLITE_API sqlite3_int64 sqlite3_column_int64(sqlite3_stmt*, int iCol);
SQLITE_API const unsigned char *sqlite3_column_text(sqlite3_stmt*, int iCol);
SQLITE_API const void *sqlite3_column_text16(sqlite3_stmt*, int iCol);
SQLITE_API int sqlite3_column_type(sqlite3_stmt*, int iCol);
SQLITE_API sqlite3_value *sqlite3_column_value(sqlite3_stmt*, int iCol);

/*
** CAPI3REF: Destroy A Prepared Statement Object
**
** ^The sqlite3_finalize() function is called to delete a [prepared statement].
** ^If the most recent evaluation of the statement encountered no errors or
** or if the statement is never been evaluated, then sqlite3_finalize() returns
** SQLITE_OK.  ^If the most recent evaluation of statement S failed, then
** sqlite3_finalize(S) returns the appropriate [error code] or
** [extended error code].
**
** ^The sqlite3_finalize(S) routine can be called at any point during
** the life cycle of [prepared statement] S:
** before statement S is ever evaluated, after
** one or more calls to [sqlite3_reset()], or after any call
** to [sqlite3_step()] regardless of whether or not the statement has
** completed execution.
**
** ^Invoking sqlite3_finalize() on a NULL pointer is a harmless no-op.
**
** The application must finalize every [prepared statement] in order to avoid
** resource leaks.  It is a grievous error for the application to try to use
** a prepared statement after it has been finalized.  Any use of a prepared
** statement after it has been finalized can result in undefined and
** undesirable behavior such as segfaults and heap corruption.
*/
SQLITE_API int sqlite3_finalize(sqlite3_stmt *pStmt);

/*
** CAPI3REF: Reset A Prepared Statement Object
**
** The sqlite3_reset() function is called to reset a [prepared statement]
** object back to its initial state, ready to be re-executed.
** ^Any SQL statement variables that had values bound to them using
** the [sqlite3_bind_blob | sqlite3_bind_*() API] retain their values.
** Use [sqlite3_clear_bindings()] to reset the bindings.
**
** ^The [sqlite3_reset(S)] interface resets the [prepared statement] S
** back to the beginning of its program.
**
** ^If the most recent call to [sqlite3_step(S)] for the
** [prepared statement] S returned [SQLITE_ROW] or [SQLITE_DONE],
** or if [sqlite3_step(S)] has never before been called on S,
** then [sqlite3_reset(S)] returns [SQLITE_OK].
**
** ^If the most recent call to [sqlite3_step(S)] for the
** [prepared statement] S indicated an error, then
** [sqlite3_reset(S)] returns an appropriate [error code].
**
** ^The [sqlite3_reset(S)] interface does not change the values
** of any [sqlite3_bind_blob|bindings] on the [prepared statement] S.
*/
SQLITE_API int sqlite3_reset(sqlite3_stmt *pStmt);

/*
** CAPI3REF: Create Or Redefine SQL Functions
** KEYWORDS: {function creation routines}
** KEYWORDS: {application-defined SQL function}
** KEYWORDS: {application-defined SQL functions}
**
** ^These functions (collectively known as "function creation routines")
** are used to add SQL functions or aggregates or to redefine the behavior
** of existing SQL functions or aggregates.  The only differences between
** these routines are the text encoding expected for
** the second parameter (the name of the function being created)
** and the presence or absence of a destructor callback for
** the application data pointer.
**
** ^The first parameter is the [database connection] to which the SQL
** function is to be added.  ^If an application uses more than one database
** connection then application-defined SQL functions must be added
** to each database connection separately.
**
** ^The second parameter is the name of the SQL function to be created or
** redefined.  ^The length of the name is limited to 255 bytes in a UTF-8
** representation, exclusive of the zero-terminator.  ^Note that the name
** length limit is in UTF-8 bytes, not characters nor UTF-16 bytes.  
** ^Any attempt to create a function with a longer name
** will result in [SQLITE_MISUSE] being returned.
**
** ^The third parameter (nArg)
** is the number of arguments that the SQL function or
** aggregate takes. ^If this parameter is -1, then the SQL function or
** aggregate may take any number of arguments between 0 and the limit
** set by [sqlite3_limit]([SQLITE_LIMIT_FUNCTION_ARG]).  If the third
** parameter is less than -1 or greater than 127 then the behavior is
** undefined.
**
** ^The fourth parameter, eTextRep, specifies what
** [SQLITE_UTF8 | text encoding] this SQL function prefers for
** its parameters.  Every SQL function implementation must be able to work
** with UTF-8, UTF-16le, or UTF-16be.  But some implementations may be
** more efficient with one encoding than another.  ^An application may
** invoke sqlite3_create_function() or sqlite3_create_function16() multiple
** times with the same function but with different values of eTextRep.
** ^When multiple implementations of the same function are available, SQLite
** will pick the one that involves the least amount of data conversion.
** If there is only a single implementation which does not care what text
** encoding is used, then the fourth argument should be [SQLITE_ANY].
**
** ^(The fifth parameter is an arbitrary pointer.  The implementation of the
** function can gain access to this pointer using [sqlite3_user_data()].)^
**
** ^The sixth, seventh and eighth parameters, xFunc, xStep and xFinal, are
** pointers to C-language functions that implement the SQL function or
** aggregate. ^A scalar SQL function requires an implementation of the xFunc
** callback only; NULL pointers must be passed as the xStep and xFinal
** parameters. ^An aggregate SQL function requires an implementation of xStep
** and xFinal and NULL pointer must be passed for xFunc. ^To delete an existing
** SQL function or aggregate, pass NULL pointers for all three function
** callbacks.
**
** ^(If the ninth parameter to sqlite3_create_function_v2() is not NULL,
** then it is destructor for the application data pointer. 
** The destructor is invoked when the function is deleted, either by being
** overloaded or when the database connection closes.)^
** ^The destructor is also invoked if the call to
** sqlite3_create_function_v2() fails.
** ^When the destructor callback of the tenth parameter is invoked, it
** is passed a single argument which is a copy of the application data 
** pointer which was the fifth parameter to sqlite3_create_function_v2().
**
** ^It is permitted to register multiple implementations of the same
** functions with the same name but with either differing numbers of
** arguments or differing preferred text encodings.  ^SQLite will use
** the implementation that most closely matches the way in which the
** SQL function is used.  ^A function implementation with a non-negative
** nArg parameter is a better match than a function implementation with
** a negative nArg.  ^A function where the preferred text encoding
** matches the database encoding is a better
** match than a function where the encoding is different.  
** ^A function where the encoding difference is between UTF16le and UTF16be
** is a closer match than a function where the encoding difference is
** between UTF8 and UTF16.
**
** ^Built-in functions may be overloaded by new application-defined functions.
**
** ^An application-defined function is permitted to call other
** SQLite interfaces.  However, such calls must not
** close the database connection nor finalize or reset the prepared
** statement in which the function is running.
*/
SQLITE_API int sqlite3_create_function(
  sqlite3 *db,
  const char *zFunctionName,
  int nArg,
  int eTextRep,
  void *pApp,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**),
  void (*xStep)(sqlite3_context*,int,sqlite3_value**),
  void (*xFinal)(sqlite3_context*)
);
SQLITE_API int sqlite3_create_function16(
  sqlite3 *db,
  const void *zFunctionName,
  int nArg,
  int eTextRep,
  void *pApp,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**),
  void (*xStep)(sqlite3_context*,int,sqlite3_value**),
  void (*xFinal)(sqlite3_context*)
);
SQLITE_API int sqlite3_create_function_v2(
  sqlite3 *db,
  const char *zFunctionName,
  int nArg,
  int eTextRep,
  void *pApp,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**),
  void (*xStep)(sqlite3_context*,int,sqlite3_value**),
  void (*xFinal)(sqlite3_context*),
  void(*xDestroy)(void*)
);

/*
** CAPI3REF: Text Encodings
**
** These constant define integer codes that represent the various
** text encodings supported by SQLite.
*/
#define SQLITE_UTF8           1
#define SQLITE_UTF16LE        2
#define SQLITE_UTF16BE        3
#define SQLITE_UTF16          4    /* Use native byte order */
#define SQLITE_ANY            5    /* sqlite3_create_function only */
#define SQLITE_UTF16_ALIGNED  8    /* sqlite3_create_collation only */

/*
** CAPI3REF: Deprecated Functions
** DEPRECATED
**
** These functions are [deprecated].  In order to maintain
** backwards compatibility with older code, these functions continue 
** to be supported.  However, new applications should avoid
** the use of these functions.  To help encourage people to avoid
** using these functions, we are not going to tell you what they do.
*/
#ifndef SQLITE_OMIT_DEPRECATED
SQLITE_API SQLITE_DEPRECATED int sqlite3_aggregate_count(sqlite3_context*);
SQLITE_API SQLITE_DEPRECATED int sqlite3_expired(sqlite3_stmt*);
SQLITE_API SQLITE_DEPRECATED int sqlite3_transfer_bindings(sqlite3_stmt*, sqlite3_stmt*);
SQLITE_API SQLITE_DEPRECATED int sqlite3_global_recover(void);
SQLITE_API SQLITE_DEPRECATED void sqlite3_thread_cleanup(void);
SQLITE_API SQLITE_DEPRECATED int sqlite3_memory_alarm(void(*)(void*,sqlite3_int64,int),void*,sqlite3_int64);
#endif

/*
** CAPI3REF: Obtaining SQL Function Parameter Values
**
** The C-language implementation of SQL functions and aggregates uses
** this set of interface routines to access the parameter values on
** the function or aggregate.
**
** The xFunc (for scalar functions) or xStep (for aggregates) parameters
** to [sqlite3_create_function()] and [sqlite3_create_function16()]
** define callbacks that implement the SQL functions and aggregates.
** The 3rd parameter to these callbacks is an array of pointers to
** [protected sqlite3_value] objects.  There is one [sqlite3_value] object for
** each parameter to the SQL function.  These routines are used to
** extract values from the [sqlite3_value] objects.
**
** These routines work only with [protected sqlite3_value] objects.
** Any attempt to use these routines on an [unprotected sqlite3_value]
** object results in undefined behavior.
**
** ^These routines work just like the corresponding [column access functions]
** except that  these routines take a single [protected sqlite3_value] object
** pointer instead of a [sqlite3_stmt*] pointer and an integer column number.
**
** ^The sqlite3_value_text16() interface extracts a UTF-16 string
** in the native byte-order of the host machine.  ^The
** sqlite3_value_text16be() and sqlite3_value_text16le() interfaces
** extract UTF-16 strings as big-endian and little-endian respectively.
**
** ^(The sqlite3_value_numeric_type() interface attempts to apply
** numeric affinity to the value.  This means that an attempt is
** made to convert the value to an integer or floating point.  If
** such a conversion is possible without loss of information (in other
** words, if the value is a string that looks like a number)
** then the conversion is performed.  Otherwise no conversion occurs.
** The [SQLITE_INTEGER | datatype] after conversion is returned.)^
**
** Please pay particular attention to the fact that the pointer returned
** from [sqlite3_value_blob()], [sqlite3_value_text()], or
** [sqlite3_value_text16()] can be invalidated by a subsequent call to
** [sqlite3_value_bytes()], [sqlite3_value_bytes16()], [sqlite3_value_text()],
** or [sqlite3_value_text16()].
**
** These routines must be called from the same thread as
** the SQL function that supplied the [sqlite3_value*] parameters.
*/
SQLITE_API const void *sqlite3_value_blob(sqlite3_value*);
SQLITE_API int sqlite3_value_bytes(sqlite3_value*);
SQLITE_API int sqlite3_value_bytes16(sqlite3_value*);
SQLITE_API double sqlite3_value_double(sqlite3_value*);
SQLITE_API int sqlite3_value_int(sqlite3_value*);
SQLITE_API sqlite3_int64 sqlite3_value_int64(sqlite3_value*);
SQLITE_API const unsigned char *sqlite3_value_text(sqlite3_value*);
SQLITE_API const void *sqlite3_value_text16(sqlite3_value*);
SQLITE_API const void *sqlite3_value_text16le(sqlite3_value*);
SQLITE_API const void *sqlite3_value_text16be(sqlite3_value*);
SQLITE_API int sqlite3_value_type(sqlite3_value*);
SQLITE_API int sqlite3_value_numeric_type(sqlite3_value*);

/*
** CAPI3REF: Obtain Aggregate Function Context
**
** Implementations of aggregate SQL functions use this
** routine to allocate memory for storing their state.
**
** ^The first time the sqlite3_aggregate_context(C,N) routine is called 
** for a particular aggregate function, SQLite
** allocates N of memory, zeroes out that memory, and returns a pointer
** to the new memory. ^On second and subsequent calls to
** sqlite3_aggregate_context() for the same aggregate function instance,
** the same buffer is returned.  Sqlite3_aggregate_context() is normally
** called once for each invocation of the xStep callback and then one
** last time when the xFinal callback is invoked.  ^(When no rows match
** an aggregate query, the xStep() callback of the aggregate function
** implementation is never called and xFinal() is called exactly once.
** In those cases, sqlite3_aggregate_context() might be called for the
** first time from within xFinal().)^
**
** ^The sqlite3_aggregate_context(C,N) routine returns a NULL pointer if N is
** less than or equal to zero or if a memory allocate error occurs.
**
** ^(The amount of space allocated by sqlite3_aggregate_context(C,N) is
** determined by the N parameter on first successful call.  Changing the
** value of N in subsequent call to sqlite3_aggregate_context() within
** the same aggregate function instance will not resize the memory
** allocation.)^
**
** ^SQLite automatically frees the memory allocated by 
** sqlite3_aggregate_context() when the aggregate query concludes.
**
** The first parameter must be a copy of the
** [sqlite3_context | SQL function context] that is the first parameter
** to the xStep or xFinal callback routine that implements the aggregate
** function.
**
** This routine must be called from the same thread in which
** the aggregate SQL function is running.
*/
SQLITE_API void *sqlite3_aggregate_context(sqlite3_context*, int nBytes);

/*
** CAPI3REF: User Data For Functions
**
** ^The sqlite3_user_data() interface returns a copy of
** the pointer that was the pUserData parameter (the 5th parameter)
** of the [sqlite3_create_function()]
** and [sqlite3_create_function16()] routines that originally
** registered the application defined function.
**
** This routine must be called from the same thread in which
** the application-defined function is running.
*/
SQLITE_API void *sqlite3_user_data(sqlite3_context*);

/*
** CAPI3REF: Database Connection For Functions
**
** ^The sqlite3_context_db_handle() interface returns a copy of
** the pointer to the [database connection] (the 1st parameter)
** of the [sqlite3_create_function()]
** and [sqlite3_create_function16()] routines that originally
** registered the application defined function.
*/
SQLITE_API sqlite3 *sqlite3_context_db_handle(sqlite3_context*);

/*
** CAPI3REF: Function Auxiliary Data
**
** The following two functions may be used by scalar SQL functions to
** associate metadata with argument values. If the same value is passed to
** multiple invocations of the same SQL function during query execution, under
** some circumstances the associated metadata may be preserved. This may
** be used, for example, to add a regular-expression matching scalar
** function. The compiled version of the regular expression is stored as
** metadata associated with the SQL value passed as the regular expression
** pattern.  The compiled regular expression can be reused on multiple
** invocations of the same function so that the original pattern string
** does not need to be recompiled on each invocation.
**
** ^The sqlite3_get_auxdata() interface returns a pointer to the metadata
** associated by the sqlite3_set_auxdata() function with the Nth argument
** value to the application-defined function. ^If no metadata has been ever
** been set for the Nth argument of the function, or if the corresponding
** function parameter has changed since the meta-data was set,
** then sqlite3_get_auxdata() returns a NULL pointer.
**
** ^The sqlite3_set_auxdata() interface saves the metadata
** pointed to by its 3rd parameter as the metadata for the N-th
** argument of the application-defined function.  Subsequent
** calls to sqlite3_get_auxdata() might return this data, if it has
** not been destroyed.
** ^If it is not NULL, SQLite will invoke the destructor
** function given by the 4th parameter to sqlite3_set_auxdata() on
** the metadata when the corresponding function parameter changes
** or when the SQL statement completes, whichever comes first.
**
** SQLite is free to call the destructor and drop metadata on any
** parameter of any function at any time.  ^The only guarantee is that
** the destructor will be called before the metadata is dropped.
**
** ^(In practice, metadata is preserved between function calls for
** expressions that are constant at compile time. This includes literal
** values and [parameters].)^
**
** These routines must be called from the same thread in which
** the SQL function is running.
*/
SQLITE_API void *sqlite3_get_auxdata(sqlite3_context*, int N);
SQLITE_API void sqlite3_set_auxdata(sqlite3_context*, int N, void*, void (*)(void*));


/*
** CAPI3REF: Constants Defining Special Destructor Behavior
**
** These are special values for the destructor that is passed in as the
** final argument to routines like [sqlite3_result_blob()].  ^If the destructor
** argument is SQLITE_STATIC, it means that the content pointer is constant
** and will never change.  It does not need to be destroyed.  ^The
** SQLITE_TRANSIENT value means that the content will likely change in
** the near future and that SQLite should make its own private copy of
** the content before returning.
**
** The typedef is necessary to work around problems in certain
** C++ compilers.  See ticket #2191.
*/
typedef void (*sqlite3_destructor_type)(void*);
#define SQLITE_STATIC      ((sqlite3_destructor_type)0)
#define SQLITE_TRANSIENT   ((sqlite3_destructor_type)-1)

/*
** CAPI3REF: Setting The Result Of An SQL Function
**
** These routines are used by the xFunc or xFinal callbacks that
** implement SQL functions and aggregates.  See
** [sqlite3_create_function()] and [sqlite3_create_function16()]
** for additional information.
**
** These functions work very much like the [parameter binding] family of
** functions used to bind values to host parameters in prepared statements.
** Refer to the [SQL parameter] documentation for additional information.
**
** ^The sqlite3_result_blob() interface sets the result from
** an application-defined function to be the BLOB whose content is pointed
** to by the second parameter and which is N bytes long where N is the
** third parameter.
**
** ^The sqlite3_result_zeroblob() interfaces set the result of
** the application-defined function to be a BLOB containing all zero
** bytes and N bytes in size, where N is the value of the 2nd parameter.
**
** ^The sqlite3_result_double() interface sets the result from
** an application-defined function to be a floating point value specified
** by its 2nd argument.
**
** ^The sqlite3_result_error() and sqlite3_result_error16() functions
** cause the implemented SQL function to throw an exception.
** ^SQLite uses the string pointed to by the
** 2nd parameter of sqlite3_result_error() or sqlite3_result_error16()
** as the text of an error message.  ^SQLite interprets the error
** message string from sqlite3_result_error() as UTF-8. ^SQLite
** interprets the string from sqlite3_result_error16() as UTF-16 in native
** byte order.  ^If the third parameter to sqlite3_result_error()
** or sqlite3_result_error16() is negative then SQLite takes as the error
** message all text up through the first zero character.
** ^If the third parameter to sqlite3_result_error() or
** sqlite3_result_error16() is non-negative then SQLite takes that many
** bytes (not characters) from the 2nd parameter as the error message.
** ^The sqlite3_result_error() and sqlite3_result_error16()
** routines make a private copy of the error message text before
** they return.  Hence, the calling function can deallocate or
** modify the text after they return without harm.
** ^The sqlite3_result_error_code() function changes the error code
** returned by SQLite as a result of an error in a function.  ^By default,
** the error code is SQLITE_ERROR.  ^A subsequent call to sqlite3_result_error()
** or sqlite3_result_error16() resets the error code to SQLITE_ERROR.
**
** ^The sqlite3_result_toobig() interface causes SQLite to throw an error
** indicating that a string or BLOB is too long to represent.
**
** ^The sqlite3_result_nomem() interface causes SQLite to throw an error
** indicating that a memory allocation failed.
**
** ^The sqlite3_result_int() interface sets the return value
** of the application-defined function to be the 32-bit signed integer
** value given in the 2nd argument.
** ^The sqlite3_result_int64() interface sets the return value
** of the application-defined function to be the 64-bit signed integer
** value given in the 2nd argument.
**
** ^The sqlite3_result_null() interface sets the return value
** of the application-defined function to be NULL.
**
** ^The sqlite3_result_text(), sqlite3_result_text16(),
** sqlite3_result_text16le(), and sqlite3_result_text16be() interfaces
** set the return value of the application-defined function to be
** a text string which is represented as UTF-8, UTF-16 native byte order,
** UTF-16 little endian, or UTF-16 big endian, respectively.
** ^SQLite takes the text result from the application from
** the 2nd parameter of the sqlite3_result_text* interfaces.
** ^If the 3rd parameter to the sqlite3_result_text* interfaces
** is negative, then SQLite takes result text from the 2nd parameter
** through the first zero character.
** ^If the 3rd parameter to the sqlite3_result_text* interfaces
** is non-negative, then as many bytes (not characters) of the text
** pointed to by the 2nd parameter are taken as the application-defined
** function result.
** ^If the 4th parameter to the sqlite3_result_text* interfaces
** or sqlite3_result_blob is a non-NULL pointer, then SQLite calls that
** function as the destructor on the text or BLOB result when it has
** finished using that result.
** ^If the 4th parameter to the sqlite3_result_text* interfaces or to
** sqlite3_result_blob is the special constant SQLITE_STATIC, then SQLite
** assumes that the text or BLOB result is in constant space and does not
** copy the content of the parameter nor call a destructor on the content
** when it has finished using that result.
** ^If the 4th parameter to the sqlite3_result_text* interfaces
** or sqlite3_result_blob is the special constant SQLITE_TRANSIENT
** then SQLite makes a copy of the result into space obtained from
** from [sqlite3_malloc()] before it returns.
**
** ^The sqlite3_result_value() interface sets the result of
** the application-defined function to be a copy the
** [unprotected sqlite3_value] object specified by the 2nd parameter.  ^The
** sqlite3_result_value() interface makes a copy of the [sqlite3_value]
** so that the [sqlite3_value] specified in the parameter may change or
** be deallocated after sqlite3_result_value() returns without harm.
** ^A [protected sqlite3_value] object may always be used where an
** [unprotected sqlite3_value] object is required, so either
** kind of [sqlite3_value] object can be used with this interface.
**
** If these routines are called from within the different thread
** than the one containing the application-defined function that received
** the [sqlite3_context] pointer, the results are undefined.
*/
SQLITE_API void sqlite3_result_blob(sqlite3_context*, const void*, int, void(*)(void*));
SQLITE_API void sqlite3_result_double(sqlite3_context*, double);
SQLITE_API void sqlite3_result_error(sqlite3_context*, const char*, int);
SQLITE_API void sqlite3_result_error16(sqlite3_context*, const void*, int);
SQLITE_API void sqlite3_result_error_toobig(sqlite3_context*);
SQLITE_API void sqlite3_result_error_nomem(sqlite3_context*);
SQLITE_API void sqlite3_result_error_code(sqlite3_context*, int);
SQLITE_API void sqlite3_result_int(sqlite3_context*, int);
SQLITE_API void sqlite3_result_int64(sqlite3_context*, sqlite3_int64);
SQLITE_API void sqlite3_result_null(sqlite3_context*);
SQLITE_API void sqlite3_result_text(sqlite3_context*, const char*, int, void(*)(void*));
SQLITE_API void sqlite3_result_text16(sqlite3_context*, const void*, int, void(*)(void*));
SQLITE_API void sqlite3_result_text16le(sqlite3_context*, const void*, int,void(*)(void*));
SQLITE_API void sqlite3_result_text16be(sqlite3_context*, const void*, int,void(*)(void*));
SQLITE_API void sqlite3_result_value(sqlite3_context*, sqlite3_value*);
SQLITE_API void sqlite3_result_zeroblob(sqlite3_context*, int n);

/*
** CAPI3REF: Define New Collating Sequences
**
** ^These functions add, remove, or modify a [collation] associated
** with the [database connection] specified as the first argument.
**
** ^The name of the collation is a UTF-8 string
** for sqlite3_create_collation() and sqlite3_create_collation_v2()
** and a UTF-16 string in native byte order for sqlite3_create_collation16().
** ^Collation names that compare equal according to [sqlite3_strnicmp()] are
** considered to be the same name.
**
** ^(The third argument (eTextRep) must be one of the constants:
** <ul>
** <li> [SQLITE_UTF8],
** <li> [SQLITE_UTF16LE],
** <li> [SQLITE_UTF16BE],
** <li> [SQLITE_UTF16], or
** <li> [SQLITE_UTF16_ALIGNED].
** </ul>)^
** ^The eTextRep argument determines the encoding of strings passed
** to the collating function callback, xCallback.
** ^The [SQLITE_UTF16] and [SQLITE_UTF16_ALIGNED] values for eTextRep
** force strings to be UTF16 with native byte order.
** ^The [SQLITE_UTF16_ALIGNED] value for eTextRep forces strings to begin
** on an even byte address.
**
** ^The fourth argument, pArg, is an application data pointer that is passed
** through as the first argument to the collating function callback.
**
** ^The fifth argument, xCallback, is a pointer to the collating function.
** ^Multiple collating functions can be registered using the same name but
** with different eTextRep parameters and SQLite will use whichever
** function requires the least amount of data transformation.
** ^If the xCallback argument is NULL then the collating function is
** deleted.  ^When all collating functions having the same name are deleted,
** that collation is no longer usable.
**
** ^The collating function callback is invoked with a copy of the pArg 
** application data pointer and with two strings in the encoding specified
** by the eTextRep argument.  The collating function must return an
** integer that is negative, zero, or positive
** if the first string is less than, equal to, or greater than the second,
** respectively.  A collating function must always return the same answer
** given the same inputs.  If two or more collating functions are registered
** to the same collation name (using different eTextRep values) then all
** must give an equivalent answer when invoked with equivalent strings.
** The collating function must obey the following properties for all
** strings A, B, and C:
**
** <ol>
** <li> If A==B then B==A.
** <li> If A==B and B==C then A==C.
** <li> If A&lt;B THEN B&gt;A.
** <li> If A&lt;B and B&lt;C then A&lt;C.
** </ol>
**
** If a collating function fails any of the above constraints and that
** collating function is  registered and used, then the behavior of SQLite
** is undefined.
**
** ^The sqlite3_create_collation_v2() works like sqlite3_create_collation()
** with the addition that the xDestroy callback is invoked on pArg when
** the collating function is deleted.
** ^Collating functions are deleted when they are overridden by later
** calls to the collation creation functions or when the
** [database connection] is closed using [sqlite3_close()].
**
** ^The xDestroy callback is <u>not</u> called if the 
** sqlite3_create_collation_v2() function fails.  Applications that invoke
** sqlite3_create_collation_v2() with a non-NULL xDestroy argument should 
** check the return code and dispose of the application data pointer
** themselves rather than expecting SQLite to deal with it for them.
** This is different from every other SQLite interface.  The inconsistency 
** is unfortunate but cannot be changed without breaking backwards 
** compatibility.
**
** See also:  [sqlite3_collation_needed()] and [sqlite3_collation_needed16()].
*/
SQLITE_API int sqlite3_create_collation(
  sqlite3*, 
  const char *zName, 
  int eTextRep, 
  void *pArg,
  int(*xCompare)(void*,int,const void*,int,const void*)
);
SQLITE_API int sqlite3_create_collation_v2(
  sqlite3*, 
  const char *zName, 
  int eTextRep, 
  void *pArg,
  int(*xCompare)(void*,int,const void*,int,const void*),
  void(*xDestroy)(void*)
);
SQLITE_API int sqlite3_create_collation16(
  sqlite3*, 
  const void *zName,
  int eTextRep, 
  void *pArg,
  int(*xCompare)(void*,int,const void*,int,const void*)
);

/*
** CAPI3REF: Collation Needed Callbacks
**
** ^To avoid having to register all collation sequences before a database
** can be used, a single callback function may be registered with the
** [database connection] to be invoked whenever an undefined collation
** sequence is required.
**
** ^If the function is registered using the sqlite3_collation_needed() API,
** then it is passed the names of undefined collation sequences as strings
** encoded in UTF-8. ^If sqlite3_collation_needed16() is used,
** the names are passed as UTF-16 in machine native byte order.
** ^A call to either function replaces the existing collation-needed callback.
**
** ^(When the callback is invoked, the first argument passed is a copy
** of the second argument to sqlite3_collation_needed() or
** sqlite3_collation_needed16().  The second argument is the database
** connection.  The third argument is one of [SQLITE_UTF8], [SQLITE_UTF16BE],
** or [SQLITE_UTF16LE], indicating the most desirable form of the collation
** sequence function required.  The fourth parameter is the name of the
** required collation sequence.)^
**
** The callback function should register the desired collation using
** [sqlite3_create_collation()], [sqlite3_create_collation16()], or
** [sqlite3_create_collation_v2()].
*/
SQLITE_API int sqlite3_collation_needed(
  sqlite3*, 
  void*, 
  void(*)(void*,sqlite3*,int eTextRep,const char*)
);
SQLITE_API int sqlite3_collation_needed16(
  sqlite3*, 
  void*,
  void(*)(void*,sqlite3*,int eTextRep,const void*)
);

#ifdef SQLITE_HAS_CODEC
/*
** Specify the key for an encrypted database.  This routine should be
** called right after sqlite3_open().
**
** The code to implement this API is not available in the public release
** of SQLite.
*/
SQLITE_API int sqlite3_key(
  sqlite3 *db,                   /* Database to be rekeyed */
  const void *pKey, int nKey     /* The key */
);

/*
** Change the key on an open database.  If the current database is not
** encrypted, this routine will encrypt it.  If pNew==0 or nNew==0, the
** database is decrypted.
**
** The code to implement this API is not available in the public release
** of SQLite.
*/
SQLITE_API int sqlite3_rekey(
  sqlite3 *db,                   /* Database to be rekeyed */
  const void *pKey, int nKey     /* The new key */
);

/*
** Specify the activation key for a SEE database.  Unless 
** activated, none of the SEE routines will work.
*/
SQLITE_API void sqlite3_activate_see(
  const char *zPassPhrase        /* Activation phrase */
);
#endif

#ifdef SQLITE_ENABLE_CEROD
/*
** Specify the activation key for a CEROD database.  Unless 
** activated, none of the CEROD routines will work.
*/
SQLITE_API void sqlite3_activate_cerod(
  const char *zPassPhrase        /* Activation phrase */
);
#endif

/*
** CAPI3REF: Suspend Execution For A Short Time
**
** The sqlite3_sleep() function causes the current thread to suspend execution
** for at least a number of milliseconds specified in its parameter.
**
** If the operating system does not support sleep requests with
** millisecond time resolution, then the time will be rounded up to
** the nearest second. The number of milliseconds of sleep actually
** requested from the operating system is returned.
**
** ^SQLite implements this interface by calling the xSleep()
** method of the default [sqlite3_vfs] object.  If the xSleep() method
** of the default VFS is not implemented correctly, or not implemented at
** all, then the behavior of sqlite3_sleep() may deviate from the description
** in the previous paragraphs.
*/
SQLITE_API int sqlite3_sleep(int);

/*
** CAPI3REF: Name Of The Folder Holding Temporary Files
**
** ^(If this global variable is made to point to a string which is
** the name of a folder (a.k.a. directory), then all temporary files
** created by SQLite when using a built-in [sqlite3_vfs | VFS]
** will be placed in that directory.)^  ^If this variable
** is a NULL pointer, then SQLite performs a search for an appropriate
** temporary file directory.
**
** It is not safe to read or modify this variable in more than one
** thread at a time.  It is not safe to read or modify this variable
** if a [database connection] is being used at the same time in a separate
** thread.
** It is intended that this variable be set once
** as part of process initialization and before any SQLite interface
** routines have been called and that this variable remain unchanged
** thereafter.
**
** ^The [temp_store_directory pragma] may modify this variable and cause
** it to point to memory obtained from [sqlite3_malloc].  ^Furthermore,
** the [temp_store_directory pragma] always assumes that any string
** that this variable points to is held in memory obtained from 
** [sqlite3_malloc] and the pragma may attempt to free that memory
** using [sqlite3_free].
** Hence, if this variable is modified directly, either it should be
** made NULL or made to point to memory obtained from [sqlite3_malloc]
** or else the use of the [temp_store_directory pragma] should be avoided.
*/
SQLITE_API extern char *sqlite3_temp_directory;

/*
** CAPI3REF: Test For Auto-Commit Mode
** KEYWORDS: {autocommit mode}
**
** ^The sqlite3_get_autocommit() interface returns non-zero or
** zero if the given database connection is or is not in autocommit mode,
** respectively.  ^Autocommit mode is on by default.
** ^Autocommit mode is disabled by a [BEGIN] statement.
** ^Autocommit mode is re-enabled by a [COMMIT] or [ROLLBACK].
**
** If certain kinds of errors occur on a statement within a multi-statement
** transaction (errors including [SQLITE_FULL], [SQLITE_IOERR],
** [SQLITE_NOMEM], [SQLITE_BUSY], and [SQLITE_INTERRUPT]) then the
** transaction might be rolled back automatically.  The only way to
** find out whether SQLite automatically rolled back the transaction after
** an error is to use this function.
**
** If another thread changes the autocommit status of the database
** connection while this routine is running, then the return value
** is undefined.
*/
SQLITE_API int sqlite3_get_autocommit(sqlite3*);

/*
** CAPI3REF: Find The Database Handle Of A Prepared Statement
**
** ^The sqlite3_db_handle interface returns the [database connection] handle
** to which a [prepared statement] belongs.  ^The [database connection]
** returned by sqlite3_db_handle is the same [database connection]
** that was the first argument
** to the [sqlite3_prepare_v2()] call (or its variants) that was used to
** create the statement in the first place.
*/
SQLITE_API sqlite3 *sqlite3_db_handle(sqlite3_stmt*);

/*
** CAPI3REF: Find the next prepared statement
**
** ^This interface returns a pointer to the next [prepared statement] after
** pStmt associated with the [database connection] pDb.  ^If pStmt is NULL
** then this interface returns a pointer to the first prepared statement
** associated with the database connection pDb.  ^If no prepared statement
** satisfies the conditions of this routine, it returns NULL.
**
** The [database connection] pointer D in a call to
** [sqlite3_next_stmt(D,S)] must refer to an open database
** connection and in particular must not be a NULL pointer.
*/
SQLITE_API sqlite3_stmt *sqlite3_next_stmt(sqlite3 *pDb, sqlite3_stmt *pStmt);

/*
** CAPI3REF: Commit And Rollback Notification Callbacks
**
** ^The sqlite3_commit_hook() interface registers a callback
** function to be invoked whenever a transaction is [COMMIT | committed].
** ^Any callback set by a previous call to sqlite3_commit_hook()
** for the same database connection is overridden.
** ^The sqlite3_rollback_hook() interface registers a callback
** function to be invoked whenever a transaction is [ROLLBACK | rolled back].
** ^Any callback set by a previous call to sqlite3_rollback_hook()
** for the same database connection is overridden.
** ^The pArg argument is passed through to the callback.
** ^If the callback on a commit hook function returns non-zero,
** then the commit is converted into a rollback.
**
** ^The sqlite3_commit_hook(D,C,P) and sqlite3_rollback_hook(D,C,P) functions
** return the P argument from the previous call of the same function
** on the same [database connection] D, or NULL for
** the first call for each function on D.
**
** The callback implementation must not do anything that will modify
** the database connection that invoked the callback.  Any actions
** to modify the database connection must be deferred until after the
** completion of the [sqlite3_step()] call that triggered the commit
** or rollback hook in the first place.
** Note that [sqlite3_prepare_v2()] and [sqlite3_step()] both modify their
** database connections for the meaning of "modify" in this paragraph.
**
** ^Registering a NULL function disables the callback.
**
** ^When the commit hook callback routine returns zero, the [COMMIT]
** operation is allowed to continue normally.  ^If the commit hook
** returns non-zero, then the [COMMIT] is converted into a [ROLLBACK].
** ^The rollback hook is invoked on a rollback that results from a commit
** hook returning non-zero, just as it would be with any other rollback.
**
** ^For the purposes of this API, a transaction is said to have been
** rolled back if an explicit "ROLLBACK" statement is executed, or
** an error or constraint causes an implicit rollback to occur.
** ^The rollback callback is not invoked if a transaction is
** automatically rolled back because the database connection is closed.
**
** See also the [sqlite3_update_hook()] interface.
*/
SQLITE_API void *sqlite3_commit_hook(sqlite3*, int(*)(void*), void*);
SQLITE_API void *sqlite3_rollback_hook(sqlite3*, void(*)(void *), void*);

/*
** CAPI3REF: Data Change Notification Callbacks
**
** ^The sqlite3_update_hook() interface registers a callback function
** with the [database connection] identified by the first argument
** to be invoked whenever a row is updated, inserted or deleted.
** ^Any callback set by a previous call to this function
** for the same database connection is overridden.
**
** ^The second argument is a pointer to the function to invoke when a
** row is updated, inserted or deleted.
** ^The first argument to the callback is a copy of the third argument
** to sqlite3_update_hook().
** ^The second callback argument is one of [SQLITE_INSERT], [SQLITE_DELETE],
** or [SQLITE_UPDATE], depending on the operation that caused the callback
** to be invoked.
** ^The third and fourth arguments to the callback contain pointers to the
** database and table name containing the affected row.
** ^The final callback parameter is the [rowid] of the row.
** ^In the case of an update, this is the [rowid] after the update takes place.
**
** ^(The update hook is not invoked when internal system tables are
** modified (i.e. sqlite_master and sqlite_sequence).)^
**
** ^In the current implementation, the update hook
** is not invoked when duplication rows are deleted because of an
** [ON CONFLICT | ON CONFLICT REPLACE] clause.  ^Nor is the update hook
** invoked when rows are deleted using the [truncate optimization].
** The exceptions defined in this paragraph might change in a future
** release of SQLite.
**
** The update hook implementation must not do anything that will modify
** the database connection that invoked the update hook.  Any actions
** to modify the database connection must be deferred until after the
** completion of the [sqlite3_step()] call that triggered the update hook.
** Note that [sqlite3_prepare_v2()] and [sqlite3_step()] both modify their
** database connections for the meaning of "modify" in this paragraph.
**
** ^The sqlite3_update_hook(D,C,P) function
** returns the P argument from the previous call
** on the same [database connection] D, or NULL for
** the first call on D.
**
** See also the [sqlite3_commit_hook()] and [sqlite3_rollback_hook()]
** interfaces.
*/
SQLITE_API void *sqlite3_update_hook(
  sqlite3*, 
  void(*)(void *,int ,char const *,char const *,sqlite3_int64),
  void*
);

/*
** CAPI3REF: Enable Or Disable Shared Pager Cache
** KEYWORDS: {shared cache}
**
** ^(This routine enables or disables the sharing of the database cache
** and schema data structures between [database connection | connections]
** to the same database. Sharing is enabled if the argument is true
** and disabled if the argument is false.)^
**
** ^Cache sharing is enabled and disabled for an entire process.
** This is a change as of SQLite version 3.5.0. In prior versions of SQLite,
** sharing was enabled or disabled for each thread separately.
**
** ^(The cache sharing mode set by this interface effects all subsequent
** calls to [sqlite3_open()], [sqlite3_open_v2()], and [sqlite3_open16()].
** Existing database connections continue use the sharing mode
** that was in effect at the time they were opened.)^
**
** ^(This routine returns [SQLITE_OK] if shared cache was enabled or disabled
** successfully.  An [error code] is returned otherwise.)^
**
** ^Shared cache is disabled by default. But this might change in
** future releases of SQLite.  Applications that care about shared
** cache setting should set it explicitly.
**
** See Also:  [SQLite Shared-Cache Mode]
*/
SQLITE_API int sqlite3_enable_shared_cache(int);

/*
** CAPI3REF: Attempt To Free Heap Memory
**
** ^The sqlite3_release_memory() interface attempts to free N bytes
** of heap memory by deallocating non-essential memory allocations
** held by the database library.   Memory used to cache database
** pages to improve performance is an example of non-essential memory.
** ^sqlite3_release_memory() returns the number of bytes actually freed,
** which might be more or less than the amount requested.
** ^The sqlite3_release_memory() routine is a no-op returning zero
** if SQLite is not compiled with [SQLITE_ENABLE_MEMORY_MANAGEMENT].
*/
SQLITE_API int sqlite3_release_memory(int);

/*
** CAPI3REF: Impose A Limit On Heap Size
**
** ^The sqlite3_soft_heap_limit64() interface sets and/or queries the
** soft limit on the amount of heap memory that may be allocated by SQLite.
** ^SQLite strives to keep heap memory utilization below the soft heap
** limit by reducing the number of pages held in the page cache
** as heap memory usages approaches the limit.
** ^The soft heap limit is "soft" because even though SQLite strives to stay
** below the limit, it will exceed the limit rather than generate
** an [SQLITE_NOMEM] error.  In other words, the soft heap limit 
** is advisory only.
**
** ^The return value from sqlite3_soft_heap_limit64() is the size of
** the soft heap limit prior to the call.  ^If the argument N is negative
** then no change is made to the soft heap limit.  Hence, the current
** size of the soft heap limit can be determined by invoking
** sqlite3_soft_heap_limit64() with a negative argument.
**
** ^If the argument N is zero then the soft heap limit is disabled.
**
** ^(The soft heap limit is not enforced in the current implementation
** if one or more of following conditions are true:
**
** <ul>
** <li> The soft heap limit is set to zero.
** <li> Memory accounting is disabled using a combination of the
**      [sqlite3_config]([SQLITE_CONFIG_MEMSTATUS],...) start-time option and
**      the [SQLITE_DEFAULT_MEMSTATUS] compile-time option.
** <li> An alternative page cache implementation is specified using
**      [sqlite3_config]([SQLITE_CONFIG_PCACHE],...).
** <li> The page cache allocates from its own memory pool supplied
**      by [sqlite3_config]([SQLITE_CONFIG_PAGECACHE],...) rather than
**      from the heap.
** </ul>)^
**
** Beginning with SQLite version 3.7.3, the soft heap limit is enforced
** regardless of whether or not the [SQLITE_ENABLE_MEMORY_MANAGEMENT]
** compile-time option is invoked.  With [SQLITE_ENABLE_MEMORY_MANAGEMENT],
** the soft heap limit is enforced on every memory allocation.  Without
** [SQLITE_ENABLE_MEMORY_MANAGEMENT], the soft heap limit is only enforced
** when memory is allocated by the page cache.  Testing suggests that because
** the page cache is the predominate memory user in SQLite, most
** applications will achieve adequate soft heap limit enforcement without
** the use of [SQLITE_ENABLE_MEMORY_MANAGEMENT].
**
** The circumstances under which SQLite will enforce the soft heap limit may
** changes in future releases of SQLite.
*/
SQLITE_API sqlite3_int64 sqlite3_soft_heap_limit64(sqlite3_int64 N);

/*
** CAPI3REF: Deprecated Soft Heap Limit Interface
** DEPRECATED
**
** This is a deprecated version of the [sqlite3_soft_heap_limit64()]
** interface.  This routine is provided for historical compatibility
** only.  All new applications should use the
** [sqlite3_soft_heap_limit64()] interface rather than this one.
*/
SQLITE_API SQLITE_DEPRECATED void sqlite3_soft_heap_limit(int N);


/*
** CAPI3REF: Extract Metadata About A Column Of A Table
**
** ^This routine returns metadata about a specific column of a specific
** database table accessible using the [database connection] handle
** passed as the first function argument.
**
** ^The column is identified by the second, third and fourth parameters to
** this function. ^The second parameter is either the name of the database
** (i.e. "main", "temp", or an attached database) containing the specified
** table or NULL. ^If it is NULL, then all attached databases are searched
** for the table using the same algorithm used by the database engine to
** resolve unqualified table references.
**
** ^The third and fourth parameters to this function are the table and column
** name of the desired column, respectively. Neither of these parameters
** may be NULL.
**
** ^Metadata is returned by writing to the memory locations passed as the 5th
** and subsequent parameters to this function. ^Any of these arguments may be
** NULL, in which case the corresponding element of metadata is omitted.
**
** ^(<blockquote>
** <table border="1">
** <tr><th> Parameter <th> Output<br>Type <th>  Description
**
** <tr><td> 5th <td> const char* <td> Data type
** <tr><td> 6th <td> const char* <td> Name of default collation sequence
** <tr><td> 7th <td> int         <td> True if column has a NOT NULL constraint
** <tr><td> 8th <td> int         <td> True if column is part of the PRIMARY KEY
** <tr><td> 9th <td> int         <td> True if column is [AUTOINCREMENT]
** </table>
** </blockquote>)^
**
** ^The memory pointed to by the character pointers returned for the
** declaration type and collation sequence is valid only until the next
** call to any SQLite API function.
**
** ^If the specified table is actually a view, an [error code] is returned.
**
** ^If the specified column is "rowid", "oid" or "_rowid_" and an
** [INTEGER PRIMARY KEY] column has been explicitly declared, then the output
** parameters are set for the explicitly declared column. ^(If there is no
** explicitly declared [INTEGER PRIMARY KEY] column, then the output
** parameters are set as follows:
**
** <pre>
**     data type: "INTEGER"
**     collation sequence: "BINARY"
**     not null: 0
**     primary key: 1
**     auto increment: 0
** </pre>)^
**
** ^(This function may load one or more schemas from database files. If an
** error occurs during this process, or if the requested table or column
** cannot be found, an [error code] is returned and an error message left
** in the [database connection] (to be retrieved using sqlite3_errmsg()).)^
**
** ^This API is only available if the library was compiled with the
** [SQLITE_ENABLE_COLUMN_METADATA] C-preprocessor symbol defined.
*/
SQLITE_API int sqlite3_table_column_metadata(
  sqlite3 *db,                /* Connection handle */
  const char *zDbName,        /* Database name or NULL */
  const char *zTableName,     /* Table name */
  const char *zColumnName,    /* Column name */
  char const **pzDataType,    /* OUTPUT: Declared data type */
  char const **pzCollSeq,     /* OUTPUT: Collation sequence name */
  int *pNotNull,              /* OUTPUT: True if NOT NULL constraint exists */
  int *pPrimaryKey,           /* OUTPUT: True if column part of PK */
  int *pAutoinc               /* OUTPUT: True if column is auto-increment */
);

/*
** CAPI3REF: Load An Extension
**
** ^This interface loads an SQLite extension library from the named file.
**
** ^The sqlite3_load_extension() interface attempts to load an
** SQLite extension library contained in the file zFile.
**
** ^The entry point is zProc.
** ^zProc may be 0, in which case the name of the entry point
** defaults to "sqlite3_extension_init".
** ^The sqlite3_load_extension() interface returns
** [SQLITE_OK] on success and [SQLITE_ERROR] if something goes wrong.
** ^If an error occurs and pzErrMsg is not 0, then the
** [sqlite3_load_extension()] interface shall attempt to
** fill *pzErrMsg with error message text stored in memory
** obtained from [sqlite3_malloc()]. The calling function
** should free this memory by calling [sqlite3_free()].
**
** ^Extension loading must be enabled using
** [sqlite3_enable_load_extension()] prior to calling this API,
** otherwise an error will be returned.
**
** See also the [load_extension() SQL function].
*/
SQLITE_API int sqlite3_load_extension(
  sqlite3 *db,          /* Load the extension into this database connection */
  const char *zFile,    /* Name of the shared library containing extension */
  const char *zProc,    /* Entry point.  Derived from zFile if 0 */
  char **pzErrMsg       /* Put error message here if not 0 */
);

/*
** CAPI3REF: Enable Or Disable Extension Loading
**
** ^So as not to open security holes in older applications that are
** unprepared to deal with extension loading, and as a means of disabling
** extension loading while evaluating user-entered SQL, the following API
** is provided to turn the [sqlite3_load_extension()] mechanism on and off.
**
** ^Extension loading is off by default. See ticket #1863.
** ^Call the sqlite3_enable_load_extension() routine with onoff==1
** to turn extension loading on and call it with onoff==0 to turn
** it back off again.
*/
SQLITE_API int sqlite3_enable_load_extension(sqlite3 *db, int onoff);

/*
** CAPI3REF: Automatically Load Statically Linked Extensions
**
** ^This interface causes the xEntryPoint() function to be invoked for
** each new [database connection] that is created.  The idea here is that
** xEntryPoint() is the entry point for a statically linked SQLite extension
** that is to be automatically loaded into all new database connections.
**
** ^(Even though the function prototype shows that xEntryPoint() takes
** no arguments and returns void, SQLite invokes xEntryPoint() with three
** arguments and expects and integer result as if the signature of the
** entry point where as follows:
**
** <blockquote><pre>
** &nbsp;  int xEntryPoint(
** &nbsp;    sqlite3 *db,
** &nbsp;    const char **pzErrMsg,
** &nbsp;    const struct sqlite3_api_routines *pThunk
** &nbsp;  );
** </pre></blockquote>)^
**
** If the xEntryPoint routine encounters an error, it should make *pzErrMsg
** point to an appropriate error message (obtained from [sqlite3_mprintf()])
** and return an appropriate [error code].  ^SQLite ensures that *pzErrMsg
** is NULL before calling the xEntryPoint().  ^SQLite will invoke
** [sqlite3_free()] on *pzErrMsg after xEntryPoint() returns.  ^If any
** xEntryPoint() returns an error, the [sqlite3_open()], [sqlite3_open16()],
** or [sqlite3_open_v2()] call that provoked the xEntryPoint() will fail.
**
** ^Calling sqlite3_auto_extension(X) with an entry point X that is already
** on the list of automatic extensions is a harmless no-op. ^No entry point
** will be called more than once for each database connection that is opened.
**
** See also: [sqlite3_reset_auto_extension()].
*/
SQLITE_API int sqlite3_auto_extension(void (*xEntryPoint)(void));

/*
** CAPI3REF: Reset Automatic Extension Loading
**
** ^This interface disables all automatic extensions previously
** registered using [sqlite3_auto_extension()].
*/
SQLITE_API void sqlite3_reset_auto_extension(void);

/*
** The interface to the virtual-table mechanism is currently considered
** to be experimental.  The interface might change in incompatible ways.
** If this is a problem for you, do not use the interface at this time.
**
** When the virtual-table mechanism stabilizes, we will declare the
** interface fixed, support it indefinitely, and remove this comment.
*/

/*
** Structures used by the virtual table interface
*/
typedef struct sqlite3_vtab sqlite3_vtab;
typedef struct sqlite3_index_info sqlite3_index_info;
typedef struct sqlite3_vtab_cursor sqlite3_vtab_cursor;
typedef struct sqlite3_module sqlite3_module;

/*
** CAPI3REF: Virtual Table Object
** KEYWORDS: sqlite3_module {virtual table module}
**
** This structure, sometimes called a "virtual table module", 
** defines the implementation of a [virtual tables].  
** This structure consists mostly of methods for the module.
**
** ^A virtual table module is created by filling in a persistent
** instance of this structure and passing a pointer to that instance
** to [sqlite3_create_module()] or [sqlite3_create_module_v2()].
** ^The registration remains valid until it is replaced by a different
** module or until the [database connection] closes.  The content
** of this structure must not change while it is registered with
** any database connection.
*/
struct sqlite3_module {
  int iVersion;
  int (*xCreate)(sqlite3*, void *pAux,
               int argc, const char *const*argv,
               sqlite3_vtab **ppVTab, char**);
  int (*xConnect)(sqlite3*, void *pAux,
               int argc, const char *const*argv,
               sqlite3_vtab **ppVTab, char**);
  int (*xBestIndex)(sqlite3_vtab *pVTab, sqlite3_index_info*);
  int (*xDisconnect)(sqlite3_vtab *pVTab);
  int (*xDestroy)(sqlite3_vtab *pVTab);
  int (*xOpen)(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor);
  int (*xClose)(sqlite3_vtab_cursor*);
  int (*xFilter)(sqlite3_vtab_cursor*, int idxNum, const char *idxStr,
                int argc, sqlite3_value **argv);
  int (*xNext)(sqlite3_vtab_cursor*);
  int (*xEof)(sqlite3_vtab_cursor*);
  int (*xColumn)(sqlite3_vtab_cursor*, sqlite3_context*, int);
  int (*xRowid)(sqlite3_vtab_cursor*, sqlite3_int64 *pRowid);
  int (*xUpdate)(sqlite3_vtab *, int, sqlite3_value **, sqlite3_int64 *);
  int (*xBegin)(sqlite3_vtab *pVTab);
  int (*xSync)(sqlite3_vtab *pVTab);
  int (*xCommit)(sqlite3_vtab *pVTab);
  int (*xRollback)(sqlite3_vtab *pVTab);
  int (*xFindFunction)(sqlite3_vtab *pVtab, int nArg, const char *zName,
                       void (**pxFunc)(sqlite3_context*,int,sqlite3_value**),
                       void **ppArg);
  int (*xRename)(sqlite3_vtab *pVtab, const char *zNew);
};

/*
** CAPI3REF: Virtual Table Indexing Information
** KEYWORDS: sqlite3_index_info
**
** The sqlite3_index_info structure and its substructures is used as part
** of the [virtual table] interface to
** pass information into and receive the reply from the [xBestIndex]
** method of a [virtual table module].  The fields under **Inputs** are the
** inputs to xBestIndex and are read-only.  xBestIndex inserts its
** results into the **Outputs** fields.
**
** ^(The aConstraint[] array records WHERE clause constraints of the form:
**
** <blockquote>column OP expr</blockquote>
**
** where OP is =, &lt;, &lt;=, &gt;, or &gt;=.)^  ^(The particular operator is
** stored in aConstraint[].op using one of the
** [SQLITE_INDEX_CONSTRAINT_EQ | SQLITE_INDEX_CONSTRAINT_ values].)^
** ^(The index of the column is stored in
** aConstraint[].iColumn.)^  ^(aConstraint[].usable is TRUE if the
** expr on the right-hand side can be evaluated (and thus the constraint
** is usable) and false if it cannot.)^
**
** ^The optimizer automatically inverts terms of the form "expr OP column"
** and makes other simplifications to the WHERE clause in an attempt to
** get as many WHERE clause terms into the form shown above as possible.
** ^The aConstraint[] array only reports WHERE clause terms that are
** relevant to the particular virtual table being queried.
**
** ^Information about the ORDER BY clause is stored in aOrderBy[].
** ^Each term of aOrderBy records a column of the ORDER BY clause.
**
** The [xBestIndex] method must fill aConstraintUsage[] with information
** about what parameters to pass to xFilter.  ^If argvIndex>0 then
** the right-hand side of the corresponding aConstraint[] is evaluated
** and becomes the argvIndex-th entry in argv.  ^(If aConstraintUsage[].omit
** is true, then the constraint is assumed to be fully handled by the
** virtual table and is not checked again by SQLite.)^
**
** ^The idxNum and idxPtr values are recorded and passed into the
** [xFilter] method.
** ^[sqlite3_free()] is used to free idxPtr if and only if
** needToFreeIdxPtr is true.
**
** ^The orderByConsumed means that output from [xFilter]/[xNext] will occur in
** the correct order to satisfy the ORDER BY clause so that no separate
** sorting step is required.
**
** ^The estimatedCost value is an estimate of the cost of doing the
** particular lookup.  A full scan of a table with N entries should have
** a cost of N.  A binary search of a table of N entries should have a
** cost of approximately log(N).
*/
  struct sqlite3_index_constraint {
     int iColumn;              /* Column on left-hand side of constraint */
     unsigned char op;         /* Constraint operator */
     unsigned char usable;     /* True if this constraint is usable */
     int iTermOffset;          /* Used internally - xBestIndex should ignore */
  };
  struct sqlite3_index_orderby {
     int iColumn;              /* Column number */
     unsigned char desc;       /* True for DESC.  False for ASC. */
  };               /* The ORDER BY clause */
  struct sqlite3_index_constraint_usage {
    int argvIndex;           /* if >0, constraint is part of argv to xFilter */
    unsigned char omit;      /* Do not code a test for this constraint */
  };

struct sqlite3_index_info {
  /* Inputs */
  int nConstraint;           /* Number of entries in aConstraint */
  struct sqlite3_index_constraint *aConstraint;            /* Table of WHERE clause constraints */
  int nOrderBy;              /* Number of terms in the ORDER BY clause */
  struct sqlite3_index_orderby *aOrderBy;               /* The ORDER BY clause */
  /* Outputs */
  struct sqlite3_index_constraint_usage *aConstraintUsage;
  int idxNum;                /* Number used to identify the index */
  char *idxStr;              /* String, possibly obtained from sqlite3_malloc */
  int needToFreeIdxStr;      /* Free idxStr using sqlite3_free() if true */
  int orderByConsumed;       /* True if output is already ordered */
  double estimatedCost;      /* Estimated cost of using this index */
};

/*
** CAPI3REF: Virtual Table Constraint Operator Codes
**
** These macros defined the allowed values for the
** [sqlite3_index_info].aConstraint[].op field.  Each value represents
** an operator that is part of a constraint term in the wHERE clause of
** a query that uses a [virtual table].
*/
#define SQLITE_INDEX_CONSTRAINT_EQ    2
#define SQLITE_INDEX_CONSTRAINT_GT    4
#define SQLITE_INDEX_CONSTRAINT_LE    8
#define SQLITE_INDEX_CONSTRAINT_LT    16
#define SQLITE_INDEX_CONSTRAINT_GE    32
#define SQLITE_INDEX_CONSTRAINT_MATCH 64

/*
** CAPI3REF: Register A Virtual Table Implementation
**
** ^These routines are used to register a new [virtual table module] name.
** ^Module names must be registered before
** creating a new [virtual table] using the module and before using a
** preexisting [virtual table] for the module.
**
** ^The module name is registered on the [database connection] specified
** by the first parameter.  ^The name of the module is given by the 
** second parameter.  ^The third parameter is a pointer to
** the implementation of the [virtual table module].   ^The fourth
** parameter is an arbitrary client data pointer that is passed through
** into the [xCreate] and [xConnect] methods of the virtual table module
** when a new virtual table is be being created or reinitialized.
**
** ^The sqlite3_create_module_v2() interface has a fifth parameter which
** is a pointer to a destructor for the pClientData.  ^SQLite will
** invoke the destructor function (if it is not NULL) when SQLite
** no longer needs the pClientData pointer.  ^The destructor will also
** be invoked if the call to sqlite3_create_module_v2() fails.
** ^The sqlite3_create_module()
** interface is equivalent to sqlite3_create_module_v2() with a NULL
** destructor.
*/
SQLITE_API int sqlite3_create_module(
  sqlite3 *db,               /* SQLite connection to register module with */
  const char *zName,         /* Name of the module */
  const sqlite3_module *p,   /* Methods for the module */
  void *pClientData          /* Client data for xCreate/xConnect */
);
SQLITE_API int sqlite3_create_module_v2(
  sqlite3 *db,               /* SQLite connection to register module with */
  const char *zName,         /* Name of the module */
  const sqlite3_module *p,   /* Methods for the module */
  void *pClientData,         /* Client data for xCreate/xConnect */
  void(*xDestroy)(void*)     /* Module destructor function */
);

/*
** CAPI3REF: Virtual Table Instance Object
** KEYWORDS: sqlite3_vtab
**
** Every [virtual table module] implementation uses a subclass
** of this object to describe a particular instance
** of the [virtual table].  Each subclass will
** be tailored to the specific needs of the module implementation.
** The purpose of this superclass is to define certain fields that are
** common to all module implementations.
**
** ^Virtual tables methods can set an error message by assigning a
** string obtained from [sqlite3_mprintf()] to zErrMsg.  The method should
** take care that any prior string is freed by a call to [sqlite3_free()]
** prior to assigning a new string to zErrMsg.  ^After the error message
** is delivered up to the client application, the string will be automatically
** freed by sqlite3_free() and the zErrMsg field will be zeroed.
*/
struct sqlite3_vtab {
  const sqlite3_module *pModule;  /* The module for this virtual table */
  int nRef;                       /* NO LONGER USED */
  char *zErrMsg;                  /* Error message from sqlite3_mprintf() */
  /* Virtual table implementations will typically add additional fields */
};

/*
** CAPI3REF: Virtual Table Cursor Object
** KEYWORDS: sqlite3_vtab_cursor {virtual table cursor}
**
** Every [virtual table module] implementation uses a subclass of the
** following structure to describe cursors that point into the
** [virtual table] and are used
** to loop through the virtual table.  Cursors are created using the
** [sqlite3_module.xOpen | xOpen] method of the module and are destroyed
** by the [sqlite3_module.xClose | xClose] method.  Cursors are used
** by the [xFilter], [xNext], [xEof], [xColumn], and [xRowid] methods
** of the module.  Each module implementation will define
** the content of a cursor structure to suit its own needs.
**
** This superclass exists in order to define fields of the cursor that
** are common to all implementations.
*/
struct sqlite3_vtab_cursor {
  sqlite3_vtab *pVtab;      /* Virtual table of this cursor */
  /* Virtual table implementations will typically add additional fields */
};

/*
** CAPI3REF: Declare The Schema Of A Virtual Table
**
** ^The [xCreate] and [xConnect] methods of a
** [virtual table module] call this interface
** to declare the format (the names and datatypes of the columns) of
** the virtual tables they implement.
*/
SQLITE_API int sqlite3_declare_vtab(sqlite3*, const char *zSQL);

/*
** CAPI3REF: Overload A Function For A Virtual Table
**
** ^(Virtual tables can provide alternative implementations of functions
** using the [xFindFunction] method of the [virtual table module].  
** But global versions of those functions
** must exist in order to be overloaded.)^
**
** ^(This API makes sure a global version of a function with a particular
** name and number of parameters exists.  If no such function exists
** before this API is called, a new function is created.)^  ^The implementation
** of the new function always causes an exception to be thrown.  So
** the new function is not good for anything by itself.  Its only
** purpose is to be a placeholder function that can be overloaded
** by a [virtual table].
*/
SQLITE_API int sqlite3_overload_function(sqlite3*, const char *zFuncName, int nArg);

/*
** The interface to the virtual-table mechanism defined above (back up
** to a comment remarkably similar to this one) is currently considered
** to be experimental.  The interface might change in incompatible ways.
** If this is a problem for you, do not use the interface at this time.
**
** When the virtual-table mechanism stabilizes, we will declare the
** interface fixed, support it indefinitely, and remove this comment.
*/

/*
** CAPI3REF: A Handle To An Open BLOB
** KEYWORDS: {BLOB handle} {BLOB handles}
**
** An instance of this object represents an open BLOB on which
** [sqlite3_blob_open | incremental BLOB I/O] can be performed.
** ^Objects of this type are created by [sqlite3_blob_open()]
** and destroyed by [sqlite3_blob_close()].
** ^The [sqlite3_blob_read()] and [sqlite3_blob_write()] interfaces
** can be used to read or write small subsections of the BLOB.
** ^The [sqlite3_blob_bytes()] interface returns the size of the BLOB in bytes.
*/
typedef struct sqlite3_blob sqlite3_blob;

/*
** CAPI3REF: Open A BLOB For Incremental I/O
**
** ^(This interfaces opens a [BLOB handle | handle] to the BLOB located
** in row iRow, column zColumn, table zTable in database zDb;
** in other words, the same BLOB that would be selected by:
**
** <pre>
**     SELECT zColumn FROM zDb.zTable WHERE [rowid] = iRow;
** </pre>)^
**
** ^If the flags parameter is non-zero, then the BLOB is opened for read
** and write access. ^If it is zero, the BLOB is opened for read access.
** ^It is not possible to open a column that is part of an index or primary 
** key for writing. ^If [foreign key constraints] are enabled, it is 
** not possible to open a column that is part of a [child key] for writing.
**
** ^Note that the database name is not the filename that contains
** the database but rather the symbolic name of the database that
** appears after the AS keyword when the database is connected using [ATTACH].
** ^For the main database file, the database name is "main".
** ^For TEMP tables, the database name is "temp".
**
** ^(On success, [SQLITE_OK] is returned and the new [BLOB handle] is written
** to *ppBlob. Otherwise an [error code] is returned and *ppBlob is set
** to be a null pointer.)^
** ^This function sets the [database connection] error code and message
** accessible via [sqlite3_errcode()] and [sqlite3_errmsg()] and related
** functions. ^Note that the *ppBlob variable is always initialized in a
** way that makes it safe to invoke [sqlite3_blob_close()] on *ppBlob
** regardless of the success or failure of this routine.
**
** ^(If the row that a BLOB handle points to is modified by an
** [UPDATE], [DELETE], or by [ON CONFLICT] side-effects
** then the BLOB handle is marked as "expired".
** This is true if any column of the row is changed, even a column
** other than the one the BLOB handle is open on.)^
** ^Calls to [sqlite3_blob_read()] and [sqlite3_blob_write()] for
** an expired BLOB handle fail with a return code of [SQLITE_ABORT].
** ^(Changes written into a BLOB prior to the BLOB expiring are not
** rolled back by the expiration of the BLOB.  Such changes will eventually
** commit if the transaction continues to completion.)^
**
** ^Use the [sqlite3_blob_bytes()] interface to determine the size of
** the opened blob.  ^The size of a blob may not be changed by this
** interface.  Use the [UPDATE] SQL command to change the size of a
** blob.
**
** ^The [sqlite3_bind_zeroblob()] and [sqlite3_result_zeroblob()] interfaces
** and the built-in [zeroblob] SQL function can be used, if desired,
** to create an empty, zero-filled blob in which to read or write using
** this interface.
**
** To avoid a resource leak, every open [BLOB handle] should eventually
** be released by a call to [sqlite3_blob_close()].
*/
SQLITE_API int sqlite3_blob_open(
  sqlite3*,
  const char *zDb,
  const char *zTable,
  const char *zColumn,
  sqlite3_int64 iRow,
  int flags,
  sqlite3_blob **ppBlob
);

/*
** CAPI3REF: Move a BLOB Handle to a New Row
**
** ^This function is used to move an existing blob handle so that it points
** to a different row of the same database table. ^The new row is identified
** by the rowid value passed as the second argument. Only the row can be
** changed. ^The database, table and column on which the blob handle is open
** remain the same. Moving an existing blob handle to a new row can be
** faster than closing the existing handle and opening a new one.
**
** ^(The new row must meet the same criteria as for [sqlite3_blob_open()] -
** it must exist and there must be either a blob or text value stored in
** the nominated column.)^ ^If the new row is not present in the table, or if
** it does not contain a blob or text value, or if another error occurs, an
** SQLite error code is returned and the blob handle is considered aborted.
** ^All subsequent calls to [sqlite3_blob_read()], [sqlite3_blob_write()] or
** [sqlite3_blob_reopen()] on an aborted blob handle immediately return
** SQLITE_ABORT. ^Calling [sqlite3_blob_bytes()] on an aborted blob handle
** always returns zero.
**
** ^This function sets the database handle error code and message.
*/
SQLITE_API int sqlite3_blob_reopen(sqlite3_blob *, sqlite3_int64);

/*
** CAPI3REF: Close A BLOB Handle
**
** ^Closes an open [BLOB handle].
**
** ^Closing a BLOB shall cause the current transaction to commit
** if there are no other BLOBs, no pending prepared statements, and the
** database connection is in [autocommit mode].
** ^If any writes were made to the BLOB, they might be held in cache
** until the close operation if they will fit.
**
** ^(Closing the BLOB often forces the changes
** out to disk and so if any I/O errors occur, they will likely occur
** at the time when the BLOB is closed.  Any errors that occur during
** closing are reported as a non-zero return value.)^
**
** ^(The BLOB is closed unconditionally.  Even if this routine returns
** an error code, the BLOB is still closed.)^
**
** ^Calling this routine with a null pointer (such as would be returned
** by a failed call to [sqlite3_blob_open()]) is a harmless no-op.
*/
SQLITE_API int sqlite3_blob_close(sqlite3_blob *);

/*
** CAPI3REF: Return The Size Of An Open BLOB
**
** ^Returns the size in bytes of the BLOB accessible via the 
** successfully opened [BLOB handle] in its only argument.  ^The
** incremental blob I/O routines can only read or overwriting existing
** blob content; they cannot change the size of a blob.
**
** This routine only works on a [BLOB handle] which has been created
** by a prior successful call to [sqlite3_blob_open()] and which has not
** been closed by [sqlite3_blob_close()].  Passing any other pointer in
** to this routine results in undefined and probably undesirable behavior.
*/
SQLITE_API int sqlite3_blob_bytes(sqlite3_blob *);

/*
** CAPI3REF: Read Data From A BLOB Incrementally
**
** ^(This function is used to read data from an open [BLOB handle] into a
** caller-supplied buffer. N bytes of data are copied into buffer Z
** from the open BLOB, starting at offset iOffset.)^
**
** ^If offset iOffset is less than N bytes from the end of the BLOB,
** [SQLITE_ERROR] is returned and no data is read.  ^If N or iOffset is
** less than zero, [SQLITE_ERROR] is returned and no data is read.
** ^The size of the blob (and hence the maximum value of N+iOffset)
** can be determined using the [sqlite3_blob_bytes()] interface.
**
** ^An attempt to read from an expired [BLOB handle] fails with an
** error code of [SQLITE_ABORT].
**
** ^(On success, sqlite3_blob_read() returns SQLITE_OK.
** Otherwise, an [error code] or an [extended error code] is returned.)^
**
** This routine only works on a [BLOB handle] which has been created
** by a prior successful call to [sqlite3_blob_open()] and which has not
** been closed by [sqlite3_blob_close()].  Passing any other pointer in
** to this routine results in undefined and probably undesirable behavior.
**
** See also: [sqlite3_blob_write()].
*/
SQLITE_API int sqlite3_blob_read(sqlite3_blob *, void *Z, int N, int iOffset);

/*
** CAPI3REF: Write Data Into A BLOB Incrementally
**
** ^This function is used to write data into an open [BLOB handle] from a
** caller-supplied buffer. ^N bytes of data are copied from the buffer Z
** into the open BLOB, starting at offset iOffset.
**
** ^If the [BLOB handle] passed as the first argument was not opened for
** writing (the flags parameter to [sqlite3_blob_open()] was zero),
** this function returns [SQLITE_READONLY].
**
** ^This function may only modify the contents of the BLOB; it is
** not possible to increase the size of a BLOB using this API.
** ^If offset iOffset is less than N bytes from the end of the BLOB,
** [SQLITE_ERROR] is returned and no data is written.  ^If N is
** less than zero [SQLITE_ERROR] is returned and no data is written.
** The size of the BLOB (and hence the maximum value of N+iOffset)
** can be determined using the [sqlite3_blob_bytes()] interface.
**
** ^An attempt to write to an expired [BLOB handle] fails with an
** error code of [SQLITE_ABORT].  ^Writes to the BLOB that occurred
** before the [BLOB handle] expired are not rolled back by the
** expiration of the handle, though of course those changes might
** have been overwritten by the statement that expired the BLOB handle
** or by other independent statements.
**
** ^(On success, sqlite3_blob_write() returns SQLITE_OK.
** Otherwise, an  [error code] or an [extended error code] is returned.)^
**
** This routine only works on a [BLOB handle] which has been created
** by a prior successful call to [sqlite3_blob_open()] and which has not
** been closed by [sqlite3_blob_close()].  Passing any other pointer in
** to this routine results in undefined and probably undesirable behavior.
**
** See also: [sqlite3_blob_read()].
*/
SQLITE_API int sqlite3_blob_write(sqlite3_blob *, const void *z, int n, int iOffset);

/*
** CAPI3REF: Virtual File System Objects
**
** A virtual filesystem (VFS) is an [sqlite3_vfs] object
** that SQLite uses to interact
** with the underlying operating system.  Most SQLite builds come with a
** single default VFS that is appropriate for the host computer.
** New VFSes can be registered and existing VFSes can be unregistered.
** The following interfaces are provided.
**
** ^The sqlite3_vfs_find() interface returns a pointer to a VFS given its name.
** ^Names are case sensitive.
** ^Names are zero-terminated UTF-8 strings.
** ^If there is no match, a NULL pointer is returned.
** ^If zVfsName is NULL then the default VFS is returned.
**
** ^New VFSes are registered with sqlite3_vfs_register().
** ^Each new VFS becomes the default VFS if the makeDflt flag is set.
** ^The same VFS can be registered multiple times without injury.
** ^To make an existing VFS into the default VFS, register it again
** with the makeDflt flag set.  If two different VFSes with the
** same name are registered, the behavior is undefined.  If a
** VFS is registered with a name that is NULL or an empty string,
** then the behavior is undefined.
**
** ^Unregister a VFS with the sqlite3_vfs_unregister() interface.
** ^(If the default VFS is unregistered, another VFS is chosen as
** the default.  The choice for the new VFS is arbitrary.)^
*/
SQLITE_API sqlite3_vfs *sqlite3_vfs_find(const char *zVfsName);
SQLITE_API int sqlite3_vfs_register(sqlite3_vfs*, int makeDflt);
SQLITE_API int sqlite3_vfs_unregister(sqlite3_vfs*);

/*
** CAPI3REF: Mutexes
**
** The SQLite core uses these routines for thread
** synchronization. Though they are intended for internal
** use by SQLite, code that links against SQLite is
** permitted to use any of these routines.
**
** The SQLite source code contains multiple implementations
** of these mutex routines.  An appropriate implementation
** is selected automatically at compile-time.  ^(The following
** implementations are available in the SQLite core:
**
** <ul>
** <li>   SQLITE_MUTEX_OS2
** <li>   SQLITE_MUTEX_PTHREAD
** <li>   SQLITE_MUTEX_W32
** <li>   SQLITE_MUTEX_NOOP
** </ul>)^
**
** ^The SQLITE_MUTEX_NOOP implementation is a set of routines
** that does no real locking and is appropriate for use in
** a single-threaded application.  ^The SQLITE_MUTEX_OS2,
** SQLITE_MUTEX_PTHREAD, and SQLITE_MUTEX_W32 implementations
** are appropriate for use on OS/2, Unix, and Windows.
**
** ^(If SQLite is compiled with the SQLITE_MUTEX_APPDEF preprocessor
** macro defined (with "-DSQLITE_MUTEX_APPDEF=1"), then no mutex
** implementation is included with the library. In this case the
** application must supply a custom mutex implementation using the
** [SQLITE_CONFIG_MUTEX] option of the sqlite3_config() function
** before calling sqlite3_initialize() or any other public sqlite3_
** function that calls sqlite3_initialize().)^
**
** ^The sqlite3_mutex_alloc() routine allocates a new
** mutex and returns a pointer to it. ^If it returns NULL
** that means that a mutex could not be allocated.  ^SQLite
** will unwind its stack and return an error.  ^(The argument
** to sqlite3_mutex_alloc() is one of these integer constants:
**
** <ul>
** <li>  SQLITE_MUTEX_FAST
** <li>  SQLITE_MUTEX_RECURSIVE
** <li>  SQLITE_MUTEX_STATIC_MASTER
** <li>  SQLITE_MUTEX_STATIC_MEM
** <li>  SQLITE_MUTEX_STATIC_MEM2
** <li>  SQLITE_MUTEX_STATIC_PRNG
** <li>  SQLITE_MUTEX_STATIC_LRU
** <li>  SQLITE_MUTEX_STATIC_LRU2
** </ul>)^
**
** ^The first two constants (SQLITE_MUTEX_FAST and SQLITE_MUTEX_RECURSIVE)
** cause sqlite3_mutex_alloc() to create
** a new mutex.  ^The new mutex is recursive when SQLITE_MUTEX_RECURSIVE
** is used but not necessarily so when SQLITE_MUTEX_FAST is used.
** The mutex implementation does not need to make a distinction
** between SQLITE_MUTEX_RECURSIVE and SQLITE_MUTEX_FAST if it does
** not want to.  ^SQLite will only request a recursive mutex in
** cases where it really needs one.  ^If a faster non-recursive mutex
** implementation is available on the host platform, the mutex subsystem
** might return such a mutex in response to SQLITE_MUTEX_FAST.
**
** ^The other allowed parameters to sqlite3_mutex_alloc() (anything other
** than SQLITE_MUTEX_FAST and SQLITE_MUTEX_RECURSIVE) each return
** a pointer to a static preexisting mutex.  ^Six static mutexes are
** used by the current version of SQLite.  Future versions of SQLite
** may add additional static mutexes.  Static mutexes are for internal
** use by SQLite only.  Applications that use SQLite mutexes should
** use only the dynamic mutexes returned by SQLITE_MUTEX_FAST or
** SQLITE_MUTEX_RECURSIVE.
**
** ^Note that if one of the dynamic mutex parameters (SQLITE_MUTEX_FAST
** or SQLITE_MUTEX_RECURSIVE) is used then sqlite3_mutex_alloc()
** returns a different mutex on every call.  ^But for the static
** mutex types, the same mutex is returned on every call that has
** the same type number.
**
** ^The sqlite3_mutex_free() routine deallocates a previously
** allocated dynamic mutex.  ^SQLite is careful to deallocate every
** dynamic mutex that it allocates.  The dynamic mutexes must not be in
** use when they are deallocated.  Attempting to deallocate a static
** mutex results in undefined behavior.  ^SQLite never deallocates
** a static mutex.
**
** ^The sqlite3_mutex_enter() and sqlite3_mutex_try() routines attempt
** to enter a mutex.  ^If another thread is already within the mutex,
** sqlite3_mutex_enter() will block and sqlite3_mutex_try() will return
** SQLITE_BUSY.  ^The sqlite3_mutex_try() interface returns [SQLITE_OK]
** upon successful entry.  ^(Mutexes created using
** SQLITE_MUTEX_RECURSIVE can be entered multiple times by the same thread.
** In such cases the,
** mutex must be exited an equal number of times before another thread
** can enter.)^  ^(If the same thread tries to enter any other
** kind of mutex more than once, the behavior is undefined.
** SQLite will never exhibit
** such behavior in its own use of mutexes.)^
**
** ^(Some systems (for example, Windows 95) do not support the operation
** implemented by sqlite3_mutex_try().  On those systems, sqlite3_mutex_try()
** will always return SQLITE_BUSY.  The SQLite core only ever uses
** sqlite3_mutex_try() as an optimization so this is acceptable behavior.)^
**
** ^The sqlite3_mutex_leave() routine exits a mutex that was
** previously entered by the same thread.   ^(The behavior
** is undefined if the mutex is not currently entered by the
** calling thread or is not currently allocated.  SQLite will
** never do either.)^
**
** ^If the argument to sqlite3_mutex_enter(), sqlite3_mutex_try(), or
** sqlite3_mutex_leave() is a NULL pointer, then all three routines
** behave as no-ops.
**
** See also: [sqlite3_mutex_held()] and [sqlite3_mutex_notheld()].
*/
SQLITE_API sqlite3_mutex *sqlite3_mutex_alloc(int);
SQLITE_API void sqlite3_mutex_free(sqlite3_mutex*);
SQLITE_API void sqlite3_mutex_enter(sqlite3_mutex*);
SQLITE_API int sqlite3_mutex_try(sqlite3_mutex*);
SQLITE_API void sqlite3_mutex_leave(sqlite3_mutex*);

/*
** CAPI3REF: Mutex Methods Object
**
** An instance of this structure defines the low-level routines
** used to allocate and use mutexes.
**
** Usually, the default mutex implementations provided by SQLite are
** sufficient, however the user has the option of substituting a custom
** implementation for specialized deployments or systems for which SQLite
** does not provide a suitable implementation. In this case, the user
** creates and populates an instance of this structure to pass
** to sqlite3_config() along with the [SQLITE_CONFIG_MUTEX] option.
** Additionally, an instance of this structure can be used as an
** output variable when querying the system for the current mutex
** implementation, using the [SQLITE_CONFIG_GETMUTEX] option.
**
** ^The xMutexInit method defined by this structure is invoked as
** part of system initialization by the sqlite3_initialize() function.
** ^The xMutexInit routine is called by SQLite exactly once for each
** effective call to [sqlite3_initialize()].
**
** ^The xMutexEnd method defined by this structure is invoked as
** part of system shutdown by the sqlite3_shutdown() function. The
** implementation of this method is expected to release all outstanding
** resources obtained by the mutex methods implementation, especially
** those obtained by the xMutexInit method.  ^The xMutexEnd()
** interface is invoked exactly once for each call to [sqlite3_shutdown()].
**
** ^(The remaining seven methods defined by this structure (xMutexAlloc,
** xMutexFree, xMutexEnter, xMutexTry, xMutexLeave, xMutexHeld and
** xMutexNotheld) implement the following interfaces (respectively):
**
** <ul>
**   <li>  [sqlite3_mutex_alloc()] </li>
**   <li>  [sqlite3_mutex_free()] </li>
**   <li>  [sqlite3_mutex_enter()] </li>
**   <li>  [sqlite3_mutex_try()] </li>
**   <li>  [sqlite3_mutex_leave()] </li>
**   <li>  [sqlite3_mutex_held()] </li>
**   <li>  [sqlite3_mutex_notheld()] </li>
** </ul>)^
**
** The only difference is that the public sqlite3_XXX functions enumerated
** above silently ignore any invocations that pass a NULL pointer instead
** of a valid mutex handle. The implementations of the methods defined
** by this structure are not required to handle this case, the results
** of passing a NULL pointer instead of a valid mutex handle are undefined
** (i.e. it is acceptable to provide an implementation that segfaults if
** it is passed a NULL pointer).
**
** The xMutexInit() method must be threadsafe.  ^It must be harmless to
** invoke xMutexInit() multiple times within the same process and without
** intervening calls to xMutexEnd().  Second and subsequent calls to
** xMutexInit() must be no-ops.
**
** ^xMutexInit() must not use SQLite memory allocation ([sqlite3_malloc()]
** and its associates).  ^Similarly, xMutexAlloc() must not use SQLite memory
** allocation for a static mutex.  ^However xMutexAlloc() may use SQLite
** memory allocation for a fast or recursive mutex.
**
** ^SQLite will invoke the xMutexEnd() method when [sqlite3_shutdown()] is
** called, but only if the prior call to xMutexInit returned SQLITE_OK.
** If xMutexInit fails in any way, it is expected to clean up after itself
** prior to returning.
*/
typedef struct sqlite3_mutex_methods sqlite3_mutex_methods;
struct sqlite3_mutex_methods {
  int (*xMutexInit)(void);
  int (*xMutexEnd)(void);
  sqlite3_mutex *(*xMutexAlloc)(int);
  void (*xMutexFree)(sqlite3_mutex *);
  void (*xMutexEnter)(sqlite3_mutex *);
  int (*xMutexTry)(sqlite3_mutex *);
  void (*xMutexLeave)(sqlite3_mutex *);
  int (*xMutexHeld)(sqlite3_mutex *);
  int (*xMutexNotheld)(sqlite3_mutex *);
};

/*
** CAPI3REF: Mutex Verification Routines
**
** The sqlite3_mutex_held() and sqlite3_mutex_notheld() routines
** are intended for use inside assert() statements.  ^The SQLite core
** never uses these routines except inside an assert() and applications
** are advised to follow the lead of the core.  ^The SQLite core only
** provides implementations for these routines when it is compiled
** with the SQLITE_DEBUG flag.  ^External mutex implementations
** are only required to provide these routines if SQLITE_DEBUG is
** defined and if NDEBUG is not defined.
**
** ^These routines should return true if the mutex in their argument
** is held or not held, respectively, by the calling thread.
**
** ^The implementation is not required to provided versions of these
** routines that actually work. If the implementation does not provide working
** versions of these routines, it should at least provide stubs that always
** return true so that one does not get spurious assertion failures.
**
** ^If the argument to sqlite3_mutex_held() is a NULL pointer then
** the routine should return 1.   This seems counter-intuitive since
** clearly the mutex cannot be held if it does not exist.  But the
** the reason the mutex does not exist is because the build is not
** using mutexes.  And we do not want the assert() containing the
** call to sqlite3_mutex_held() to fail, so a non-zero return is
** the appropriate thing to do.  ^The sqlite3_mutex_notheld()
** interface should also return 1 when given a NULL pointer.
*/
#ifndef NDEBUG
SQLITE_API int sqlite3_mutex_held(sqlite3_mutex*);
SQLITE_API int sqlite3_mutex_notheld(sqlite3_mutex*);
#endif

/*
** CAPI3REF: Mutex Types
**
** The [sqlite3_mutex_alloc()] interface takes a single argument
** which is one of these integer constants.
**
** The set of static mutexes may change from one SQLite release to the
** next.  Applications that override the built-in mutex logic must be
** prepared to accommodate additional static mutexes.
*/
#define SQLITE_MUTEX_FAST             0
#define SQLITE_MUTEX_RECURSIVE        1
#define SQLITE_MUTEX_STATIC_MASTER    2
#define SQLITE_MUTEX_STATIC_MEM       3  /* sqlite3_malloc() */
#define SQLITE_MUTEX_STATIC_MEM2      4  /* NOT USED */
#define SQLITE_MUTEX_STATIC_OPEN      4  /* sqlite3BtreeOpen() */
#define SQLITE_MUTEX_STATIC_PRNG      5  /* sqlite3_random() */
#define SQLITE_MUTEX_STATIC_LRU       6  /* lru page list */
#define SQLITE_MUTEX_STATIC_LRU2      7  /* NOT USED */
#define SQLITE_MUTEX_STATIC_PMEM      7  /* sqlite3PageMalloc() */

/*
** CAPI3REF: Retrieve the mutex for a database connection
**
** ^This interface returns a pointer the [sqlite3_mutex] object that 
** serializes access to the [database connection] given in the argument
** when the [threading mode] is Serialized.
** ^If the [threading mode] is Single-thread or Multi-thread then this
** routine returns a NULL pointer.
*/
SQLITE_API sqlite3_mutex *sqlite3_db_mutex(sqlite3*);

/*
** CAPI3REF: Low-Level Control Of Database Files
**
** ^The [sqlite3_file_control()] interface makes a direct call to the
** xFileControl method for the [sqlite3_io_methods] object associated
** with a particular database identified by the second argument. ^The
** name of the database is "main" for the main database or "temp" for the
** TEMP database, or the name that appears after the AS keyword for
** databases that are added using the [ATTACH] SQL command.
** ^A NULL pointer can be used in place of "main" to refer to the
** main database file.
** ^The third and fourth parameters to this routine
** are passed directly through to the second and third parameters of
** the xFileControl method.  ^The return value of the xFileControl
** method becomes the return value of this routine.
**
** ^The SQLITE_FCNTL_FILE_POINTER value for the op parameter causes
** a pointer to the underlying [sqlite3_file] object to be written into
** the space pointed to by the 4th parameter.  ^The SQLITE_FCNTL_FILE_POINTER
** case is a short-circuit path which does not actually invoke the
** underlying sqlite3_io_methods.xFileControl method.
**
** ^If the second parameter (zDbName) does not match the name of any
** open database file, then SQLITE_ERROR is returned.  ^This error
** code is not remembered and will not be recalled by [sqlite3_errcode()]
** or [sqlite3_errmsg()].  The underlying xFileControl method might
** also return SQLITE_ERROR.  There is no way to distinguish between
** an incorrect zDbName and an SQLITE_ERROR return from the underlying
** xFileControl method.
**
** See also: [SQLITE_FCNTL_LOCKSTATE]
*/
SQLITE_API int sqlite3_file_control(sqlite3*, const char *zDbName, int op, void*);

/*
** CAPI3REF: Testing Interface
**
** ^The sqlite3_test_control() interface is used to read out internal
** state of SQLite and to inject faults into SQLite for testing
** purposes.  ^The first parameter is an operation code that determines
** the number, meaning, and operation of all subsequent parameters.
**
** This interface is not for use by applications.  It exists solely
** for verifying the correct operation of the SQLite library.  Depending
** on how the SQLite library is compiled, this interface might not exist.
**
** The details of the operation codes, their meanings, the parameters
** they take, and what they do are all subject to change without notice.
** Unlike most of the SQLite API, this function is not guaranteed to
** operate consistently from one release to the next.
*/
SQLITE_API int sqlite3_test_control(int op, ...);

/*
** CAPI3REF: Testing Interface Operation Codes
**
** These constants are the valid operation code parameters used
** as the first argument to [sqlite3_test_control()].
**
** These parameters and their meanings are subject to change
** without notice.  These values are for testing purposes only.
** Applications should not use any of these parameters or the
** [sqlite3_test_control()] interface.
*/
#define SQLITE_TESTCTRL_FIRST                    5
#define SQLITE_TESTCTRL_PRNG_SAVE                5
#define SQLITE_TESTCTRL_PRNG_RESTORE             6
#define SQLITE_TESTCTRL_PRNG_RESET               7
#define SQLITE_TESTCTRL_BITVEC_TEST              8
#define SQLITE_TESTCTRL_FAULT_INSTALL            9
#define SQLITE_TESTCTRL_BENIGN_MALLOC_HOOKS     10
#define SQLITE_TESTCTRL_PENDING_BYTE            11
#define SQLITE_TESTCTRL_ASSERT                  12
#define SQLITE_TESTCTRL_ALWAYS                  13
#define SQLITE_TESTCTRL_RESERVE                 14
#define SQLITE_TESTCTRL_OPTIMIZATIONS           15
#define SQLITE_TESTCTRL_ISKEYWORD               16
#define SQLITE_TESTCTRL_PGHDRSZ                 17
#define SQLITE_TESTCTRL_SCRATCHMALLOC           18
#define SQLITE_TESTCTRL_LAST                    18

/*
** CAPI3REF: SQLite Runtime Status
**
** ^This interface is used to retrieve runtime status information
** about the performance of SQLite, and optionally to reset various
** highwater marks.  ^The first argument is an integer code for
** the specific parameter to measure.  ^(Recognized integer codes
** are of the form [SQLITE_STATUS_MEMORY_USED | SQLITE_STATUS_...].)^
** ^The current value of the parameter is returned into *pCurrent.
** ^The highest recorded value is returned in *pHighwater.  ^If the
** resetFlag is true, then the highest record value is reset after
** *pHighwater is written.  ^(Some parameters do not record the highest
** value.  For those parameters
** nothing is written into *pHighwater and the resetFlag is ignored.)^
** ^(Other parameters record only the highwater mark and not the current
** value.  For these latter parameters nothing is written into *pCurrent.)^
**
** ^The sqlite3_status() routine returns SQLITE_OK on success and a
** non-zero [error code] on failure.
**
** This routine is threadsafe but is not atomic.  This routine can be
** called while other threads are running the same or different SQLite
** interfaces.  However the values returned in *pCurrent and
** *pHighwater reflect the status of SQLite at different points in time
** and it is possible that another thread might change the parameter
** in between the times when *pCurrent and *pHighwater are written.
**
** See also: [sqlite3_db_status()]
*/
SQLITE_API int sqlite3_status(int op, int *pCurrent, int *pHighwater, int resetFlag);


/*
** CAPI3REF: Status Parameters
**
** These integer constants designate various run-time status parameters
** that can be returned by [sqlite3_status()].
**
** <dl>
** ^(<dt>SQLITE_STATUS_MEMORY_USED</dt>
** <dd>This parameter is the current amount of memory checked out
** using [sqlite3_malloc()], either directly or indirectly.  The
** figure includes calls made to [sqlite3_malloc()] by the application
** and internal memory usage by the SQLite library.  Scratch memory
** controlled by [SQLITE_CONFIG_SCRATCH] and auxiliary page-cache
** memory controlled by [SQLITE_CONFIG_PAGECACHE] is not included in
** this parameter.  The amount returned is the sum of the allocation
** sizes as reported by the xSize method in [sqlite3_mem_methods].</dd>)^
**
** ^(<dt>SQLITE_STATUS_MALLOC_SIZE</dt>
** <dd>This parameter records the largest memory allocation request
** handed to [sqlite3_malloc()] or [sqlite3_realloc()] (or their
** internal equivalents).  Only the value returned in the
** *pHighwater parameter to [sqlite3_status()] is of interest.  
** The value written into the *pCurrent parameter is undefined.</dd>)^
**
** ^(<dt>SQLITE_STATUS_MALLOC_COUNT</dt>
** <dd>This parameter records the number of separate memory allocations
** currently checked out.</dd>)^
**
** ^(<dt>SQLITE_STATUS_PAGECACHE_USED</dt>
** <dd>This parameter returns the number of pages used out of the
** [pagecache memory allocator] that was configured using 
** [SQLITE_CONFIG_PAGECACHE].  The
** value returned is in pages, not in bytes.</dd>)^
**
** ^(<dt>SQLITE_STATUS_PAGECACHE_OVERFLOW</dt>
** <dd>This parameter returns the number of bytes of page cache
** allocation which could not be satisfied by the [SQLITE_CONFIG_PAGECACHE]
** buffer and where forced to overflow to [sqlite3_malloc()].  The
** returned value includes allocations that overflowed because they
** where too large (they were larger than the "sz" parameter to
** [SQLITE_CONFIG_PAGECACHE]) and allocations that overflowed because
** no space was left in the page cache.</dd>)^
**
** ^(<dt>SQLITE_STATUS_PAGECACHE_SIZE</dt>
** <dd>This parameter records the largest memory allocation request
** handed to [pagecache memory allocator].  Only the value returned in the
** *pHighwater parameter to [sqlite3_status()] is of interest.  
** The value written into the *pCurrent parameter is undefined.</dd>)^
**
** ^(<dt>SQLITE_STATUS_SCRATCH_USED</dt>
** <dd>This parameter returns the number of allocations used out of the
** [scratch memory allocator] configured using
** [SQLITE_CONFIG_SCRATCH].  The value returned is in allocations, not
** in bytes.  Since a single thread may only have one scratch allocation
** outstanding at time, this parameter also reports the number of threads
** using scratch memory at the same time.</dd>)^
**
** ^(<dt>SQLITE_STATUS_SCRATCH_OVERFLOW</dt>
** <dd>This parameter returns the number of bytes of scratch memory
** allocation which could not be satisfied by the [SQLITE_CONFIG_SCRATCH]
** buffer and where forced to overflow to [sqlite3_malloc()].  The values
** returned include overflows because the requested allocation was too
** larger (that is, because the requested allocation was larger than the
** "sz" parameter to [SQLITE_CONFIG_SCRATCH]) and because no scratch buffer
** slots were available.
** </dd>)^
**
** ^(<dt>SQLITE_STATUS_SCRATCH_SIZE</dt>
** <dd>This parameter records the largest memory allocation request
** handed to [scratch memory allocator].  Only the value returned in the
** *pHighwater parameter to [sqlite3_status()] is of interest.  
** The value written into the *pCurrent parameter is undefined.</dd>)^
**
** ^(<dt>SQLITE_STATUS_PARSER_STACK</dt>
** <dd>This parameter records the deepest parser stack.  It is only
** meaningful if SQLite is compiled with [YYTRACKMAXSTACKDEPTH].</dd>)^
** </dl>
**
** New status parameters may be added from time to time.
*/
#define SQLITE_STATUS_MEMORY_USED          0
#define SQLITE_STATUS_PAGECACHE_USED       1
#define SQLITE_STATUS_PAGECACHE_OVERFLOW   2
#define SQLITE_STATUS_SCRATCH_USED         3
#define SQLITE_STATUS_SCRATCH_OVERFLOW     4
#define SQLITE_STATUS_MALLOC_SIZE          5
#define SQLITE_STATUS_PARSER_STACK         6
#define SQLITE_STATUS_PAGECACHE_SIZE       7
#define SQLITE_STATUS_SCRATCH_SIZE         8
#define SQLITE_STATUS_MALLOC_COUNT         9

/*
** CAPI3REF: Database Connection Status
**
** ^This interface is used to retrieve runtime status information 
** about a single [database connection].  ^The first argument is the
** database connection object to be interrogated.  ^The second argument
** is an integer constant, taken from the set of
** [SQLITE_DBSTATUS_LOOKASIDE_USED | SQLITE_DBSTATUS_*] macros, that
** determines the parameter to interrogate.  The set of 
** [SQLITE_DBSTATUS_LOOKASIDE_USED | SQLITE_DBSTATUS_*] macros is likely
** to grow in future releases of SQLite.
**
** ^The current value of the requested parameter is written into *pCur
** and the highest instantaneous value is written into *pHiwtr.  ^If
** the resetFlg is true, then the highest instantaneous value is
** reset back down to the current value.
**
** ^The sqlite3_db_status() routine returns SQLITE_OK on success and a
** non-zero [error code] on failure.
**
** See also: [sqlite3_status()] and [sqlite3_stmt_status()].
*/
SQLITE_API int sqlite3_db_status(sqlite3*, int op, int *pCur, int *pHiwtr, int resetFlg);

/*
** CAPI3REF: Status Parameters for database connections
**
** These constants are the available integer "verbs" that can be passed as
** the second argument to the [sqlite3_db_status()] interface.
**
** New verbs may be added in future releases of SQLite. Existing verbs
** might be discontinued. Applications should check the return code from
** [sqlite3_db_status()] to make sure that the call worked.
** The [sqlite3_db_status()] interface will return a non-zero error code
** if a discontinued or unsupported verb is invoked.
**
** <dl>
** ^(<dt>SQLITE_DBSTATUS_LOOKASIDE_USED</dt>
** <dd>This parameter returns the number of lookaside memory slots currently
** checked out.</dd>)^
**
** ^(<dt>SQLITE_DBSTATUS_LOOKASIDE_HIT</dt>
** <dd>This parameter returns the number malloc attempts that were 
** satisfied using lookaside memory. Only the high-water value is meaningful;
** the current value is always zero.)^
**
** ^(<dt>SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE</dt>
** <dd>This parameter returns the number malloc attempts that might have
** been satisfied using lookaside memory but failed due to the amount of
** memory requested being larger than the lookaside slot size.
** Only the high-water value is meaningful;
** the current value is always zero.)^
**
** ^(<dt>SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL</dt>
** <dd>This parameter returns the number malloc attempts that might have
** been satisfied using lookaside memory but failed due to all lookaside
** memory already being in use.
** Only the high-water value is meaningful;
** the current value is always zero.)^
**
** ^(<dt>SQLITE_DBSTATUS_CACHE_USED</dt>
** <dd>This parameter returns the approximate number of of bytes of heap
** memory used by all pager caches associated with the database connection.)^
** ^The highwater mark associated with SQLITE_DBSTATUS_CACHE_USED is always 0.
**
** ^(<dt>SQLITE_DBSTATUS_SCHEMA_USED</dt>
** <dd>This parameter returns the approximate number of of bytes of heap
** memory used to store the schema for all databases associated
** with the connection - main, temp, and any [ATTACH]-ed databases.)^ 
** ^The full amount of memory used by the schemas is reported, even if the
** schema memory is shared with other database connections due to
** [shared cache mode] being enabled.
** ^The highwater mark associated with SQLITE_DBSTATUS_SCHEMA_USED is always 0.
**
** ^(<dt>SQLITE_DBSTATUS_STMT_USED</dt>
** <dd>This parameter returns the approximate number of of bytes of heap
** and lookaside memory used by all prepared statements associated with
** the database connection.)^
** ^The highwater mark associated with SQLITE_DBSTATUS_STMT_USED is always 0.
** </dd>
** </dl>
*/
#define SQLITE_DBSTATUS_LOOKASIDE_USED       0
#define SQLITE_DBSTATUS_CACHE_USED           1
#define SQLITE_DBSTATUS_SCHEMA_USED          2
#define SQLITE_DBSTATUS_STMT_USED            3
#define SQLITE_DBSTATUS_LOOKASIDE_HIT        4
#define SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE  5
#define SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL  6
#define SQLITE_DBSTATUS_MAX                  6   /* Largest defined DBSTATUS */


/*
** CAPI3REF: Prepared Statement Status
**
** ^(Each prepared statement maintains various
** [SQLITE_STMTSTATUS_SORT | counters] that measure the number
** of times it has performed specific operations.)^  These counters can
** be used to monitor the performance characteristics of the prepared
** statements.  For example, if the number of table steps greatly exceeds
** the number of table searches or result rows, that would tend to indicate
** that the prepared statement is using a full table scan rather than
** an index.  
**
** ^(This interface is used to retrieve and reset counter values from
** a [prepared statement].  The first argument is the prepared statement
** object to be interrogated.  The second argument
** is an integer code for a specific [SQLITE_STMTSTATUS_SORT | counter]
** to be interrogated.)^
** ^The current value of the requested counter is returned.
** ^If the resetFlg is true, then the counter is reset to zero after this
** interface call returns.
**
** See also: [sqlite3_status()] and [sqlite3_db_status()].
*/
SQLITE_API int sqlite3_stmt_status(sqlite3_stmt*, int op,int resetFlg);

/*
** CAPI3REF: Status Parameters for prepared statements
**
** These preprocessor macros define integer codes that name counter
** values associated with the [sqlite3_stmt_status()] interface.
** The meanings of the various counters are as follows:
**
** <dl>
** <dt>SQLITE_STMTSTATUS_FULLSCAN_STEP</dt>
** <dd>^This is the number of times that SQLite has stepped forward in
** a table as part of a full table scan.  Large numbers for this counter
** may indicate opportunities for performance improvement through 
** careful use of indices.</dd>
**
** <dt>SQLITE_STMTSTATUS_SORT</dt>
** <dd>^This is the number of sort operations that have occurred.
** A non-zero value in this counter may indicate an opportunity to
** improvement performance through careful use of indices.</dd>
**
** <dt>SQLITE_STMTSTATUS_AUTOINDEX</dt>
** <dd>^This is the number of rows inserted into transient indices that
** were created automatically in order to help joins run faster.
** A non-zero value in this counter may indicate an opportunity to
** improvement performance by adding permanent indices that do not
** need to be reinitialized each time the statement is run.</dd>
**
** </dl>
*/
#define SQLITE_STMTSTATUS_FULLSCAN_STEP     1
#define SQLITE_STMTSTATUS_SORT              2
#define SQLITE_STMTSTATUS_AUTOINDEX         3

/*
** CAPI3REF: Custom Page Cache Object
**
** The sqlite3_pcache type is opaque.  It is implemented by
** the pluggable module.  The SQLite core has no knowledge of
** its size or internal structure and never deals with the
** sqlite3_pcache object except by holding and passing pointers
** to the object.
**
** See [sqlite3_pcache_methods] for additional information.
*/
typedef struct sqlite3_pcache sqlite3_pcache;

/*
** CAPI3REF: Application Defined Page Cache.
** KEYWORDS: {page cache}
**
** ^(The [sqlite3_config]([SQLITE_CONFIG_PCACHE], ...) interface can
** register an alternative page cache implementation by passing in an 
** instance of the sqlite3_pcache_methods structure.)^
** In many applications, most of the heap memory allocated by 
** SQLite is used for the page cache.
** By implementing a 
** custom page cache using this API, an application can better control
** the amount of memory consumed by SQLite, the way in which 
** that memory is allocated and released, and the policies used to 
** determine exactly which parts of a database file are cached and for 
** how long.
**
** The alternative page cache mechanism is an
** extreme measure that is only needed by the most demanding applications.
** The built-in page cache is recommended for most uses.
**
** ^(The contents of the sqlite3_pcache_methods structure are copied to an
** internal buffer by SQLite within the call to [sqlite3_config].  Hence
** the application may discard the parameter after the call to
** [sqlite3_config()] returns.)^
**
** ^(The xInit() method is called once for each effective 
** call to [sqlite3_initialize()])^
** (usually only once during the lifetime of the process). ^(The xInit()
** method is passed a copy of the sqlite3_pcache_methods.pArg value.)^
** The intent of the xInit() method is to set up global data structures 
** required by the custom page cache implementation. 
** ^(If the xInit() method is NULL, then the 
** built-in default page cache is used instead of the application defined
** page cache.)^
**
** ^The xShutdown() method is called by [sqlite3_shutdown()].
** It can be used to clean up 
** any outstanding resources before process shutdown, if required.
** ^The xShutdown() method may be NULL.
**
** ^SQLite automatically serializes calls to the xInit method,
** so the xInit method need not be threadsafe.  ^The
** xShutdown method is only called from [sqlite3_shutdown()] so it does
** not need to be threadsafe either.  All other methods must be threadsafe
** in multithreaded applications.
**
** ^SQLite will never invoke xInit() more than once without an intervening
** call to xShutdown().
**
** ^SQLite invokes the xCreate() method to construct a new cache instance.
** SQLite will typically create one cache instance for each open database file,
** though this is not guaranteed. ^The
** first parameter, szPage, is the size in bytes of the pages that must
** be allocated by the cache.  ^szPage will not be a power of two.  ^szPage
** will the page size of the database file that is to be cached plus an
** increment (here called "R") of less than 250.  SQLite will use the
** extra R bytes on each page to store metadata about the underlying
** database page on disk.  The value of R depends
** on the SQLite version, the target platform, and how SQLite was compiled.
** ^(R is constant for a particular build of SQLite. Except, there are two
** distinct values of R when SQLite is compiled with the proprietary
** ZIPVFS extension.)^  ^The second argument to
** xCreate(), bPurgeable, is true if the cache being created will
** be used to cache database pages of a file stored on disk, or
** false if it is used for an in-memory database. The cache implementation
** does not have to do anything special based with the value of bPurgeable;
** it is purely advisory.  ^On a cache where bPurgeable is false, SQLite will
** never invoke xUnpin() except to deliberately delete a page.
** ^In other words, calls to xUnpin() on a cache with bPurgeable set to
** false will always have the "discard" flag set to true.  
** ^Hence, a cache created with bPurgeable false will
** never contain any unpinned pages.
**
** ^(The xCachesize() method may be called at any time by SQLite to set the
** suggested maximum cache-size (number of pages stored by) the cache
** instance passed as the first argument. This is the value configured using
** the SQLite "[PRAGMA cache_size]" command.)^  As with the bPurgeable
** parameter, the implementation is not required to do anything with this
** value; it is advisory only.
**
** The xPagecount() method must return the number of pages currently
** stored in the cache, both pinned and unpinned.
** 
** The xFetch() method locates a page in the cache and returns a pointer to 
** the page, or a NULL pointer.
** A "page", in this context, means a buffer of szPage bytes aligned at an
** 8-byte boundary. The page to be fetched is determined by the key. ^The
** mimimum key value is 1.  After it has been retrieved using xFetch, the page 
** is considered to be "pinned".
**
** If the requested page is already in the page cache, then the page cache
** implementation must return a pointer to the page buffer with its content
** intact.  If the requested page is not already in the cache, then the
** cache implementation should use the value of the createFlag
** parameter to help it determined what action to take:
**
** <table border=1 width=85% align=center>
** <tr><th> createFlag <th> Behaviour when page is not already in cache
** <tr><td> 0 <td> Do not allocate a new page.  Return NULL.
** <tr><td> 1 <td> Allocate a new page if it easy and convenient to do so.
**                 Otherwise return NULL.
** <tr><td> 2 <td> Make every effort to allocate a new page.  Only return
**                 NULL if allocating a new page is effectively impossible.
** </table>
**
** ^(SQLite will normally invoke xFetch() with a createFlag of 0 or 1.  SQLite
** will only use a createFlag of 2 after a prior call with a createFlag of 1
** failed.)^  In between the to xFetch() calls, SQLite may
** attempt to unpin one or more cache pages by spilling the content of
** pinned pages to disk and synching the operating system disk cache.
**
** ^xUnpin() is called by SQLite with a pointer to a currently pinned page
** as its second argument.  If the third parameter, discard, is non-zero,
** then the page must be evicted from the cache.
** ^If the discard parameter is
** zero, then the page may be discarded or retained at the discretion of
** page cache implementation. ^The page cache implementation
** may choose to evict unpinned pages at any time.
**
** The cache must not perform any reference counting. A single 
** call to xUnpin() unpins the page regardless of the number of prior calls 
** to xFetch().
**
** The xRekey() method is used to change the key value associated with the
** page passed as the second argument. If the cache
** previously contains an entry associated with newKey, it must be
** discarded. ^Any prior cache entry associated with newKey is guaranteed not
** to be pinned.
**
** When SQLite calls the xTruncate() method, the cache must discard all
** existing cache entries with page numbers (keys) greater than or equal
** to the value of the iLimit parameter passed to xTruncate(). If any
** of these pages are pinned, they are implicitly unpinned, meaning that
** they can be safely discarded.
**
** ^The xDestroy() method is used to delete a cache allocated by xCreate().
** All resources associated with the specified cache should be freed. ^After
** calling the xDestroy() method, SQLite considers the [sqlite3_pcache*]
** handle invalid, and will not use it with any other sqlite3_pcache_methods
** functions.
*/
typedef struct sqlite3_pcache_methods sqlite3_pcache_methods;
struct sqlite3_pcache_methods {
  void *pArg;
  int (*xInit)(void*);
  void (*xShutdown)(void*);
  sqlite3_pcache *(*xCreate)(int szPage, int bPurgeable);
  void (*xCachesize)(sqlite3_pcache*, int nCachesize);
  int (*xPagecount)(sqlite3_pcache*);
  void *(*xFetch)(sqlite3_pcache*, unsigned key, int createFlag);
  void (*xUnpin)(sqlite3_pcache*, void*, int discard);
  void (*xRekey)(sqlite3_pcache*, void*, unsigned oldKey, unsigned newKey);
  void (*xTruncate)(sqlite3_pcache*, unsigned iLimit);
  void (*xDestroy)(sqlite3_pcache*);
};

/*
** CAPI3REF: Online Backup Object
**
** The sqlite3_backup object records state information about an ongoing
** online backup operation.  ^The sqlite3_backup object is created by
** a call to [sqlite3_backup_init()] and is destroyed by a call to
** [sqlite3_backup_finish()].
**
** See Also: [Using the SQLite Online Backup API]
*/
typedef struct sqlite3_backup sqlite3_backup;

/*
** CAPI3REF: Online Backup API.
**
** The backup API copies the content of one database into another.
** It is useful either for creating backups of databases or
** for copying in-memory databases to or from persistent files. 
**
** See Also: [Using the SQLite Online Backup API]
**
** ^SQLite holds a write transaction open on the destination database file
** for the duration of the backup operation.
** ^The source database is read-locked only while it is being read;
** it is not locked continuously for the entire backup operation.
** ^Thus, the backup may be performed on a live source database without
** preventing other database connections from
** reading or writing to the source database while the backup is underway.
** 
** ^(To perform a backup operation: 
**   <ol>
**     <li><b>sqlite3_backup_init()</b> is called once to initialize the
**         backup, 
**     <li><b>sqlite3_backup_step()</b> is called one or more times to transfer 
**         the data between the two databases, and finally
**     <li><b>sqlite3_backup_finish()</b> is called to release all resources 
**         associated with the backup operation. 
**   </ol>)^
** There should be exactly one call to sqlite3_backup_finish() for each
** successful call to sqlite3_backup_init().
**
** <b>sqlite3_backup_init()</b>
**
** ^The D and N arguments to sqlite3_backup_init(D,N,S,M) are the 
** [database connection] associated with the destination database 
** and the database name, respectively.
** ^The database name is "main" for the main database, "temp" for the
** temporary database, or the name specified after the AS keyword in
** an [ATTACH] statement for an attached database.
** ^The S and M arguments passed to 
** sqlite3_backup_init(D,N,S,M) identify the [database connection]
** and database name of the source database, respectively.
** ^The source and destination [database connections] (parameters S and D)
** must be different or else sqlite3_backup_init(D,N,S,M) will fail with
** an error.
**
** ^If an error occurs within sqlite3_backup_init(D,N,S,M), then NULL is
** returned and an error code and error message are stored in the
** destination [database connection] D.
** ^The error code and message for the failed call to sqlite3_backup_init()
** can be retrieved using the [sqlite3_errcode()], [sqlite3_errmsg()], and/or
** [sqlite3_errmsg16()] functions.
** ^A successful call to sqlite3_backup_init() returns a pointer to an
** [sqlite3_backup] object.
** ^The [sqlite3_backup] object may be used with the sqlite3_backup_step() and
** sqlite3_backup_finish() functions to perform the specified backup 
** operation.
**
** <b>sqlite3_backup_step()</b>
**
** ^Function sqlite3_backup_step(B,N) will copy up to N pages between 
** the source and destination databases specified by [sqlite3_backup] object B.
** ^If N is negative, all remaining source pages are copied. 
** ^If sqlite3_backup_step(B,N) successfully copies N pages and there
** are still more pages to be copied, then the function returns [SQLITE_OK].
** ^If sqlite3_backup_step(B,N) successfully finishes copying all pages
** from source to destination, then it returns [SQLITE_DONE].
** ^If an error occurs while running sqlite3_backup_step(B,N),
** then an [error code] is returned. ^As well as [SQLITE_OK] and
** [SQLITE_DONE], a call to sqlite3_backup_step() may return [SQLITE_READONLY],
** [SQLITE_NOMEM], [SQLITE_BUSY], [SQLITE_LOCKED], or an
** [SQLITE_IOERR_ACCESS | SQLITE_IOERR_XXX] extended error code.
**
** ^(The sqlite3_backup_step() might return [SQLITE_READONLY] if
** <ol>
** <li> the destination database was opened read-only, or
** <li> the destination database is using write-ahead-log journaling
** and the destination and source page sizes differ, or
** <li> the destination database is an in-memory database and the
** destination and source page sizes differ.
** </ol>)^
**
** ^If sqlite3_backup_step() cannot obtain a required file-system lock, then
** the [sqlite3_busy_handler | busy-handler function]
** is invoked (if one is specified). ^If the 
** busy-handler returns non-zero before the lock is available, then 
** [SQLITE_BUSY] is returned to the caller. ^In this case the call to
** sqlite3_backup_step() can be retried later. ^If the source
** [database connection]
** is being used to write to the source database when sqlite3_backup_step()
** is called, then [SQLITE_LOCKED] is returned immediately. ^Again, in this
** case the call to sqlite3_backup_step() can be retried later on. ^(If
** [SQLITE_IOERR_ACCESS | SQLITE_IOERR_XXX], [SQLITE_NOMEM], or
** [SQLITE_READONLY] is returned, then 
** there is no point in retrying the call to sqlite3_backup_step(). These 
** errors are considered fatal.)^  The application must accept 
** that the backup operation has failed and pass the backup operation handle 
** to the sqlite3_backup_finish() to release associated resources.
**
** ^The first call to sqlite3_backup_step() obtains an exclusive lock
** on the destination file. ^The exclusive lock is not released until either 
** sqlite3_backup_finish() is called or the backup operation is complete 
** and sqlite3_backup_step() returns [SQLITE_DONE].  ^Every call to
** sqlite3_backup_step() obtains a [shared lock] on the source database that
** lasts for the duration of the sqlite3_backup_step() call.
** ^Because the source database is not locked between calls to
** sqlite3_backup_step(), the source database may be modified mid-way
** through the backup process.  ^If the source database is modified by an
** external process or via a database connection other than the one being
** used by the backup operation, then the backup will be automatically
** restarted by the next call to sqlite3_backup_step(). ^If the source 
** database is modified by the using the same database connection as is used
** by the backup operation, then the backup database is automatically
** updated at the same time.
**
** <b>sqlite3_backup_finish()</b>
**
** When sqlite3_backup_step() has returned [SQLITE_DONE], or when the 
** application wishes to abandon the backup operation, the application
** should destroy the [sqlite3_backup] by passing it to sqlite3_backup_finish().
** ^The sqlite3_backup_finish() interfaces releases all
** resources associated with the [sqlite3_backup] object. 
** ^If sqlite3_backup_step() has not yet returned [SQLITE_DONE], then any
** active write-transaction on the destination database is rolled back.
** The [sqlite3_backup] object is invalid
** and may not be used following a call to sqlite3_backup_finish().
**
** ^The value returned by sqlite3_backup_finish is [SQLITE_OK] if no
** sqlite3_backup_step() errors occurred, regardless or whether or not
** sqlite3_backup_step() completed.
** ^If an out-of-memory condition or IO error occurred during any prior
** sqlite3_backup_step() call on the same [sqlite3_backup] object, then
** sqlite3_backup_finish() returns the corresponding [error code].
**
** ^A return of [SQLITE_BUSY] or [SQLITE_LOCKED] from sqlite3_backup_step()
** is not a permanent error and does not affect the return value of
** sqlite3_backup_finish().
**
** <b>sqlite3_backup_remaining(), sqlite3_backup_pagecount()</b>
**
** ^Each call to sqlite3_backup_step() sets two values inside
** the [sqlite3_backup] object: the number of pages still to be backed
** up and the total number of pages in the source database file.
** The sqlite3_backup_remaining() and sqlite3_backup_pagecount() interfaces
** retrieve these two values, respectively.
**
** ^The values returned by these functions are only updated by
** sqlite3_backup_step(). ^If the source database is modified during a backup
** operation, then the values are not updated to account for any extra
** pages that need to be updated or the size of the source database file
** changing.
**
** <b>Concurrent Usage of Database Handles</b>
**
** ^The source [database connection] may be used by the application for other
** purposes while a backup operation is underway or being initialized.
** ^If SQLite is compiled and configured to support threadsafe database
** connections, then the source database connection may be used concurrently
** from within other threads.
**
** However, the application must guarantee that the destination 
** [database connection] is not passed to any other API (by any thread) after 
** sqlite3_backup_init() is called and before the corresponding call to
** sqlite3_backup_finish().  SQLite does not currently check to see
** if the application incorrectly accesses the destination [database connection]
** and so no error code is reported, but the operations may malfunction
** nevertheless.  Use of the destination database connection while a
** backup is in progress might also also cause a mutex deadlock.
**
** If running in [shared cache mode], the application must
** guarantee that the shared cache used by the destination database
** is not accessed while the backup is running. In practice this means
** that the application must guarantee that the disk file being 
** backed up to is not accessed by any connection within the process,
** not just the specific connection that was passed to sqlite3_backup_init().
**
** The [sqlite3_backup] object itself is partially threadsafe. Multiple 
** threads may safely make multiple concurrent calls to sqlite3_backup_step().
** However, the sqlite3_backup_remaining() and sqlite3_backup_pagecount()
** APIs are not strictly speaking threadsafe. If they are invoked at the
** same time as another thread is invoking sqlite3_backup_step() it is
** possible that they return invalid values.
*/
SQLITE_API sqlite3_backup *sqlite3_backup_init(
  sqlite3 *pDest,                        /* Destination database handle */
  const char *zDestName,                 /* Destination database name */
  sqlite3 *pSource,                      /* Source database handle */
  const char *zSourceName                /* Source database name */
);
SQLITE_API int sqlite3_backup_step(sqlite3_backup *p, int nPage);
SQLITE_API int sqlite3_backup_finish(sqlite3_backup *p);
SQLITE_API int sqlite3_backup_remaining(sqlite3_backup *p);
SQLITE_API int sqlite3_backup_pagecount(sqlite3_backup *p);

/*
** CAPI3REF: Unlock Notification
**
** ^When running in shared-cache mode, a database operation may fail with
** an [SQLITE_LOCKED] error if the required locks on the shared-cache or
** individual tables within the shared-cache cannot be obtained. See
** [SQLite Shared-Cache Mode] for a description of shared-cache locking. 
** ^This API may be used to register a callback that SQLite will invoke 
** when the connection currently holding the required lock relinquishes it.
** ^This API is only available if the library was compiled with the
** [SQLITE_ENABLE_UNLOCK_NOTIFY] C-preprocessor symbol defined.
**
** See Also: [Using the SQLite Unlock Notification Feature].
**
** ^Shared-cache locks are released when a database connection concludes
** its current transaction, either by committing it or rolling it back. 
**
** ^When a connection (known as the blocked connection) fails to obtain a
** shared-cache lock and SQLITE_LOCKED is returned to the caller, the
** identity of the database connection (the blocking connection) that
** has locked the required resource is stored internally. ^After an 
** application receives an SQLITE_LOCKED error, it may call the
** sqlite3_unlock_notify() method with the blocked connection handle as 
** the first argument to register for a callback that will be invoked
** when the blocking connections current transaction is concluded. ^The
** callback is invoked from within the [sqlite3_step] or [sqlite3_close]
** call that concludes the blocking connections transaction.
**
** ^(If sqlite3_unlock_notify() is called in a multi-threaded application,
** there is a chance that the blocking connection will have already
** concluded its transaction by the time sqlite3_unlock_notify() is invoked.
** If this happens, then the specified callback is invoked immediately,
** from within the call to sqlite3_unlock_notify().)^
**
** ^If the blocked connection is attempting to obtain a write-lock on a
** shared-cache table, and more than one other connection currently holds
** a read-lock on the same table, then SQLite arbitrarily selects one of 
** the other connections to use as the blocking connection.
**
** ^(There may be at most one unlock-notify callback registered by a 
** blocked connection. If sqlite3_unlock_notify() is called when the
** blocked connection already has a registered unlock-notify callback,
** then the new callback replaces the old.)^ ^If sqlite3_unlock_notify() is
** called with a NULL pointer as its second argument, then any existing
** unlock-notify callback is canceled. ^The blocked connections 
** unlock-notify callback may also be canceled by closing the blocked
** connection using [sqlite3_close()].
**
** The unlock-notify callback is not reentrant. If an application invokes
** any sqlite3_xxx API functions from within an unlock-notify callback, a
** crash or deadlock may be the result.
**
** ^Unless deadlock is detected (see below), sqlite3_unlock_notify() always
** returns SQLITE_OK.
**
** <b>Callback Invocation Details</b>
**
** When an unlock-notify callback is registered, the application provides a 
** single void* pointer that is passed to the callback when it is invoked.
** However, the signature of the callback function allows SQLite to pass
** it an array of void* context pointers. The first argument passed to
** an unlock-notify callback is a pointer to an array of void* pointers,
** and the second is the number of entries in the array.
**
** When a blocking connections transaction is concluded, there may be
** more than one blocked connection that has registered for an unlock-notify
** callback. ^If two or more such blocked connections have specified the
** same callback function, then instead of invoking the callback function
** multiple times, it is invoked once with the set of void* context pointers
** specified by the blocked connections bundled together into an array.
** This gives the application an opportunity to prioritize any actions 
** related to the set of unblocked database connections.
**
** <b>Deadlock Detection</b>
**
** Assuming that after registering for an unlock-notify callback a 
** database waits for the callback to be issued before taking any further
** action (a reasonable assumption), then using this API may cause the
** application to deadlock. For example, if connection X is waiting for
** connection Y's transaction to be concluded, and similarly connection
** Y is waiting on connection X's transaction, then neither connection
** will proceed and the system may remain deadlocked indefinitely.
**
** To avoid this scenario, the sqlite3_unlock_notify() performs deadlock
** detection. ^If a given call to sqlite3_unlock_notify() would put the
** system in a deadlocked state, then SQLITE_LOCKED is returned and no
** unlock-notify callback is registered. The system is said to be in
** a deadlocked state if connection A has registered for an unlock-notify
** callback on the conclusion of connection B's transaction, and connection
** B has itself registered for an unlock-notify callback when connection
** A's transaction is concluded. ^Indirect deadlock is also detected, so
** the system is also considered to be deadlocked if connection B has
** registered for an unlock-notify callback on the conclusion of connection
** C's transaction, where connection C is waiting on connection A. ^Any
** number of levels of indirection are allowed.
**
** <b>The "DROP TABLE" Exception</b>
**
** When a call to [sqlite3_step()] returns SQLITE_LOCKED, it is almost 
** always appropriate to call sqlite3_unlock_notify(). There is however,
** one exception. When executing a "DROP TABLE" or "DROP INDEX" statement,
** SQLite checks if there are any currently executing SELECT statements
** that belong to the same connection. If there are, SQLITE_LOCKED is
** returned. In this case there is no "blocking connection", so invoking
** sqlite3_unlock_notify() results in the unlock-notify callback being
** invoked immediately. If the application then re-attempts the "DROP TABLE"
** or "DROP INDEX" query, an infinite loop might be the result.
**
** One way around this problem is to check the extended error code returned
** by an sqlite3_step() call. ^(If there is a blocking connection, then the
** extended error code is set to SQLITE_LOCKED_SHAREDCACHE. Otherwise, in
** the special "DROP TABLE/INDEX" case, the extended error code is just 
** SQLITE_LOCKED.)^
*/
SQLITE_API int sqlite3_unlock_notify(
  sqlite3 *pBlocked,                          /* Waiting connection */
  void (*xNotify)(void **apArg, int nArg),    /* Callback function to invoke */
  void *pNotifyArg                            /* Argument to pass to xNotify */
);


/*
** CAPI3REF: String Comparison
**
** ^The [sqlite3_strnicmp()] API allows applications and extensions to
** compare the contents of two buffers containing UTF-8 strings in a
** case-independent fashion, using the same definition of case independence 
** that SQLite uses internally when comparing identifiers.
*/
SQLITE_API int sqlite3_strnicmp(const char *, const char *, int);

/*
** CAPI3REF: Error Logging Interface
**
** ^The [sqlite3_log()] interface writes a message into the error log
** established by the [SQLITE_CONFIG_LOG] option to [sqlite3_config()].
** ^If logging is enabled, the zFormat string and subsequent arguments are
** used with [sqlite3_snprintf()] to generate the final output string.
**
** The sqlite3_log() interface is intended for use by extensions such as
** virtual tables, collating functions, and SQL functions.  While there is
** nothing to prevent an application from calling sqlite3_log(), doing so
** is considered bad form.
**
** The zFormat string must not be NULL.
**
** To avoid deadlocks and other threading problems, the sqlite3_log() routine
** will not use dynamically allocated memory.  The log message is stored in
** a fixed-length buffer on the stack.  If the log message is longer than
** a few hundred characters, it will be truncated to the length of the
** buffer.
*/
SQLITE_API void sqlite3_log(int iErrCode, const char *zFormat, ...);

/*
** CAPI3REF: Write-Ahead Log Commit Hook
**
** ^The [sqlite3_wal_hook()] function is used to register a callback that
** will be invoked each time a database connection commits data to a
** [write-ahead log] (i.e. whenever a transaction is committed in
** [journal_mode | journal_mode=WAL mode]). 
**
** ^The callback is invoked by SQLite after the commit has taken place and 
** the associated write-lock on the database released, so the implementation 
** may read, write or [checkpoint] the database as required.
**
** ^The first parameter passed to the callback function when it is invoked
** is a copy of the third parameter passed to sqlite3_wal_hook() when
** registering the callback. ^The second is a copy of the database handle.
** ^The third parameter is the name of the database that was written to -
** either "main" or the name of an [ATTACH]-ed database. ^The fourth parameter
** is the number of pages currently in the write-ahead log file,
** including those that were just committed.
**
** The callback function should normally return [SQLITE_OK].  ^If an error
** code is returned, that error will propagate back up through the
** SQLite code base to cause the statement that provoked the callback
** to report an error, though the commit will have still occurred. If the
** callback returns [SQLITE_ROW] or [SQLITE_DONE], or if it returns a value
** that does not correspond to any valid SQLite error code, the results
** are undefined.
**
** A single database handle may have at most a single write-ahead log callback 
** registered at one time. ^Calling [sqlite3_wal_hook()] replaces any
** previously registered write-ahead log callback. ^Note that the
** [sqlite3_wal_autocheckpoint()] interface and the
** [wal_autocheckpoint pragma] both invoke [sqlite3_wal_hook()] and will
** those overwrite any prior [sqlite3_wal_hook()] settings.
*/
SQLITE_API void *sqlite3_wal_hook(
  sqlite3*, 
  int(*)(void *,sqlite3*,const char*,int),
  void*
);

/*
** CAPI3REF: Configure an auto-checkpoint
**
** ^The [sqlite3_wal_autocheckpoint(D,N)] is a wrapper around
** [sqlite3_wal_hook()] that causes any database on [database connection] D
** to automatically [checkpoint]
** after committing a transaction if there are N or
** more frames in the [write-ahead log] file.  ^Passing zero or 
** a negative value as the nFrame parameter disables automatic
** checkpoints entirely.
**
** ^The callback registered by this function replaces any existing callback
** registered using [sqlite3_wal_hook()].  ^Likewise, registering a callback
** using [sqlite3_wal_hook()] disables the automatic checkpoint mechanism
** configured by this function.
**
** ^The [wal_autocheckpoint pragma] can be used to invoke this interface
** from SQL.
**
** ^Every new [database connection] defaults to having the auto-checkpoint
** enabled with a threshold of 1000 or [SQLITE_DEFAULT_WAL_AUTOCHECKPOINT]
** pages.  The use of this interface
** is only necessary if the default setting is found to be suboptimal
** for a particular application.
*/
SQLITE_API int sqlite3_wal_autocheckpoint(sqlite3 *db, int N);

/*
** CAPI3REF: Checkpoint a database
**
** ^The [sqlite3_wal_checkpoint(D,X)] interface causes database named X
** on [database connection] D to be [checkpointed].  ^If X is NULL or an
** empty string, then a checkpoint is run on all databases of
** connection D.  ^If the database connection D is not in
** [WAL | write-ahead log mode] then this interface is a harmless no-op.
**
** ^The [wal_checkpoint pragma] can be used to invoke this interface
** from SQL.  ^The [sqlite3_wal_autocheckpoint()] interface and the
** [wal_autocheckpoint pragma] can be used to cause this interface to be
** run whenever the WAL reaches a certain size threshold.
**
** See also: [sqlite3_wal_checkpoint_v2()]
*/
SQLITE_API int sqlite3_wal_checkpoint(sqlite3 *db, const char *zDb);

/*
** CAPI3REF: Checkpoint a database
**
** Run a checkpoint operation on WAL database zDb attached to database 
** handle db. The specific operation is determined by the value of the 
** eMode parameter:
**
** <dl>
** <dt>SQLITE_CHECKPOINT_PASSIVE<dd>
**   Checkpoint as many frames as possible without waiting for any database 
**   readers or writers to finish. Sync the db file if all frames in the log
**   are checkpointed. This mode is the same as calling 
**   sqlite3_wal_checkpoint(). The busy-handler callback is never invoked.
**
** <dt>SQLITE_CHECKPOINT_FULL<dd>
**   This mode blocks (calls the busy-handler callback) until there is no
**   database writer and all readers are reading from the most recent database
**   snapshot. It then checkpoints all frames in the log file and syncs the
**   database file. This call blocks database writers while it is running,
**   but not database readers.
**
** <dt>SQLITE_CHECKPOINT_RESTART<dd>
**   This mode works the same way as SQLITE_CHECKPOINT_FULL, except after 
**   checkpointing the log file it blocks (calls the busy-handler callback)
**   until all readers are reading from the database file only. This ensures 
**   that the next client to write to the database file restarts the log file 
**   from the beginning. This call blocks database writers while it is running,
**   but not database readers.
** </dl>
**
** If pnLog is not NULL, then *pnLog is set to the total number of frames in
** the log file before returning. If pnCkpt is not NULL, then *pnCkpt is set to
** the total number of checkpointed frames (including any that were already
** checkpointed when this function is called). *pnLog and *pnCkpt may be
** populated even if sqlite3_wal_checkpoint_v2() returns other than SQLITE_OK.
** If no values are available because of an error, they are both set to -1
** before returning to communicate this to the caller.
**
** All calls obtain an exclusive "checkpoint" lock on the database file. If
** any other process is running a checkpoint operation at the same time, the 
** lock cannot be obtained and SQLITE_BUSY is returned. Even if there is a 
** busy-handler configured, it will not be invoked in this case.
**
** The SQLITE_CHECKPOINT_FULL and RESTART modes also obtain the exclusive 
** "writer" lock on the database file. If the writer lock cannot be obtained
** immediately, and a busy-handler is configured, it is invoked and the writer
** lock retried until either the busy-handler returns 0 or the lock is
** successfully obtained. The busy-handler is also invoked while waiting for
** database readers as described above. If the busy-handler returns 0 before
** the writer lock is obtained or while waiting for database readers, the
** checkpoint operation proceeds from that point in the same way as 
** SQLITE_CHECKPOINT_PASSIVE - checkpointing as many frames as possible 
** without blocking any further. SQLITE_BUSY is returned in this case.
**
** If parameter zDb is NULL or points to a zero length string, then the
** specified operation is attempted on all WAL databases. In this case the
** values written to output parameters *pnLog and *pnCkpt are undefined. If 
** an SQLITE_BUSY error is encountered when processing one or more of the 
** attached WAL databases, the operation is still attempted on any remaining 
** attached databases and SQLITE_BUSY is returned to the caller. If any other 
** error occurs while processing an attached database, processing is abandoned 
** and the error code returned to the caller immediately. If no error 
** (SQLITE_BUSY or otherwise) is encountered while processing the attached 
** databases, SQLITE_OK is returned.
**
** If database zDb is the name of an attached database that is not in WAL
** mode, SQLITE_OK is returned and both *pnLog and *pnCkpt set to -1. If
** zDb is not NULL (or a zero length string) and is not the name of any
** attached database, SQLITE_ERROR is returned to the caller.
*/
SQLITE_API int sqlite3_wal_checkpoint_v2(
  sqlite3 *db,                    /* Database handle */
  const char *zDb,                /* Name of attached database (or NULL) */
  int eMode,                      /* SQLITE_CHECKPOINT_* value */
  int *pnLog,                     /* OUT: Size of WAL log in frames */
  int *pnCkpt                     /* OUT: Total number of frames checkpointed */
);

/*
** CAPI3REF: Checkpoint operation parameters
**
** These constants can be used as the 3rd parameter to
** [sqlite3_wal_checkpoint_v2()].  See the [sqlite3_wal_checkpoint_v2()]
** documentation for additional information about the meaning and use of
** each of these values.
*/
#define SQLITE_CHECKPOINT_PASSIVE 0
#define SQLITE_CHECKPOINT_FULL    1
#define SQLITE_CHECKPOINT_RESTART 2


/*
** Undo the hack that converts floating point types to integer for
** builds on processors without floating point support.
*/
#ifdef SQLITE_OMIT_FLOATING_POINT
# undef double
#endif

#if 0
}  /* End of the 'extern "C"' block */
#endif
#endif

/*
** 2010 August 30
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/

#ifndef _SQLITE3RTREE_H_
#define _SQLITE3RTREE_H_


#if 0
extern "C" {
#endif

typedef struct sqlite3_rtree_geometry sqlite3_rtree_geometry;

/*
** Register a geometry callback named zGeom that can be used as part of an
** R-Tree geometry query as follows:
**
**   SELECT ... FROM <rtree> WHERE <rtree col> MATCH $zGeom(... params ...)
*/
SQLITE_API int sqlite3_rtree_geometry_callback(
  sqlite3 *db,
  const char *zGeom,
  int (*xGeom)(sqlite3_rtree_geometry *, int nCoord, double *aCoord, int *pRes),
  void *pContext
);


/*
** A pointer to a structure of the following type is passed as the first
** argument to callbacks registered using rtree_geometry_callback().
*/
struct sqlite3_rtree_geometry {
  void *pContext;                 /* Copy of pContext passed to s_r_g_c() */
  int nParam;                     /* Size of array aParam[] */
  double *aParam;                 /* Parameters passed to SQL geom function */
  void *pUser;                    /* Callback implementation user data */
  void (*xDelUser)(void *);       /* Called by SQLite to clean up pUser */
};


#if 0
}  /* end of the 'extern "C"' block */
#endif

#endif  /* ifndef _SQLITE3RTREE_H_ */


/************** End of sqlite3.h *********************************************/
/************** Continuing where we left off in sqliteInt.h ******************/
/************** Include hash.h in the middle of sqliteInt.h ******************/
/************** Begin file hash.h ********************************************/
/*
** 2001 September 22
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This is the header file for the generic hash-table implemenation
** used in SQLite.
*/
#ifndef _SQLITE_HASH_H_
#define _SQLITE_HASH_H_

/* Forward declarations of structures. */
typedef struct Hash Hash;
typedef struct HashElem HashElem;

/* A complete hash table is an instance of the following structure.
** The internals of this structure are intended to be opaque -- client
** code should not attempt to access or modify the fields of this structure
** directly.  Change this structure only by using the routines below.
** However, some of the "procedures" and "functions" for modifying and
** accessing this structure are really macros, so we can't really make
** this structure opaque.
**
** All elements of the hash table are on a single doubly-linked list.
** Hash.first points to the head of this list.
**
** There are Hash.htsize buckets.  Each bucket points to a spot in
** the global doubly-linked list.  The contents of the bucket are the
** element pointed to plus the next _ht.count-1 elements in the list.
**
** Hash.htsize and Hash.ht may be zero.  In that case lookup is done
** by a linear search of the global list.  For small tables, the 
** Hash.ht table is never allocated because if there are few elements
** in the table, it is faster to do a linear search than to manage
** the hash table.
*/
  struct _ht {              /* the hash table */
    int count;                 /* Number of entries with this hash */
    HashElem *chain;           /* Pointer to first entry with this hash */
  };
struct Hash {
  unsigned int htsize;      /* Number of buckets in the hash table */
  unsigned int count;       /* Number of entries in this table */
  HashElem *first;          /* The first element of the array */
  struct _ht *ht;
};

/* Each element in the hash table is an instance of the following 
** structure.  All elements are stored on a single doubly-linked list.
**
** Again, this structure is intended to be opaque, but it can't really
** be opaque because it is used by macros.
*/
struct HashElem {
  HashElem *next, *prev;       /* Next and previous elements in the table */
  void *data;                  /* Data associated with this element */
  const char *pKey; int nKey;  /* Key associated with this element */
};

/*
** Access routines.  To delete, insert a NULL pointer.
*/
SQLITE_PRIVATE void sqlite3HashInit(Hash*);
SQLITE_PRIVATE void *sqlite3HashInsert(Hash*, const char *pKey, int nKey, void *pData);
SQLITE_PRIVATE void *sqlite3HashFind(const Hash*, const char *pKey, int nKey);
SQLITE_PRIVATE void sqlite3HashClear(Hash*);

/*
** Macros for looping over all elements of a hash table.  The idiom is
** like this:
**
**   Hash h;
**   HashElem *p;
**   ...
**   for(p=sqliteHashFirst(&h); p; p=sqliteHashNext(p)){
**     SomeStructure *pData = sqliteHashData(p);
**     // do something with pData
**   }
*/
#define sqliteHashFirst(H)  ((H)->first)
#define sqliteHashNext(E)   ((E)->next)
#define sqliteHashData(E)   ((E)->data)
/* #define sqliteHashKey(E)    ((E)->pKey) // NOT USED */
/* #define sqliteHashKeysize(E) ((E)->nKey)  // NOT USED */

/*
** Number of entries in a hash table
*/
/* #define sqliteHashCount(H)  ((H)->count) // NOT USED */

#endif /* _SQLITE_HASH_H_ */

/************** End of hash.h ************************************************/
/************** Continuing where we left off in sqliteInt.h ******************/
/************** Include parse.h in the middle of sqliteInt.h *****************/
/************** Begin file parse.h *******************************************/
#define TK_SEMI                            1
#define TK_EXPLAIN                         2
#define TK_QUERY                           3
#define TK_PLAN                            4
#define TK_BEGIN                           5
#define TK_TRANSACTION                     6
#define TK_DEFERRED                        7
#define TK_IMMEDIATE                       8
#define TK_EXCLUSIVE                       9
#define TK_COMMIT                         10
#define TK_END                            11
#define TK_ROLLBACK                       12
#define TK_SAVEPOINT                      13
#define TK_RELEASE                        14
#define TK_TO                             15
#define TK_TABLE                          16
#define TK_CREATE                         17
#define TK_IF                             18
#define TK_NOT                            19
#define TK_EXISTS                         20
#define TK_TEMP                           21
#define TK_LP                             22
#define TK_RP                             23
#define TK_AS                             24
#define TK_COMMA                          25
#define TK_ID                             26
#define TK_INDEXED                        27
#define TK_ABORT                          28
#define TK_ACTION                         29
#define TK_AFTER                          30
#define TK_ANALYZE                        31
#define TK_ASC                            32
#define TK_ATTACH                         33
#define TK_BEFORE                         34
#define TK_BY                             35
#define TK_CASCADE                        36
#define TK_CAST                           37
#define TK_COLUMNKW                       38
#define TK_CONFLICT                       39
#define TK_DATABASE                       40
#define TK_DESC                           41
#define TK_DETACH                         42
#define TK_EACH                           43
#define TK_FAIL                           44
#define TK_FOR                            45
#define TK_IGNORE                         46
#define TK_INITIALLY                      47
#define TK_INSTEAD                        48
#define TK_LIKE_KW                        49
#define TK_MATCH                          50
#define TK_NO                             51
#define TK_KEY                            52
#define TK_OF                             53
#define TK_OFFSET                         54
#define TK_PRAGMA                         55
#define TK_RAISE                          56
#define TK_REPLACE                        57
#define TK_RESTRICT                       58
#define TK_ROW                            59
#define TK_TRIGGER                        60
#define TK_VACUUM                         61
#define TK_VIEW                           62
#define TK_VIRTUAL                        63
#define TK_REINDEX                        64
#define TK_RENAME                         65
#define TK_CTIME_KW                       66
#define TK_ANY                            67
#define TK_OR                             68
#define TK_AND                            69
#define TK_IS                             70
#define TK_BETWEEN                        71
#define TK_IN                             72
#define TK_ISNULL                         73
#define TK_NOTNULL                        74
#define TK_NE                             75
#define TK_EQ                             76
#define TK_GT                             77
#define TK_LE                             78
#define TK_LT                             79
#define TK_GE                             80
#define TK_ESCAPE                         81
#define TK_BITAND                         82
#define TK_BITOR                          83
#define TK_LSHIFT                         84
#define TK_RSHIFT                         85
#define TK_PLUS                           86
#define TK_MINUS                          87
#define TK_STAR                           88
#define TK_SLASH                          89
#define TK_REM                            90
#define TK_CONCAT                         91
#define TK_COLLATE                        92
#define TK_BITNOT                         93
#define TK_STRING                         94
#define TK_JOIN_KW                        95
#define TK_CONSTRAINT                     96
#define TK_DEFAULT                        97
#define TK_NULL                           98
#define TK_PRIMARY                        99
#define TK_UNIQUE                         100
#define TK_CHECK                          101
#define TK_REFERENCES                     102
#define TK_AUTOINCR                       103
#define TK_ON                             104
#define TK_INSERT                         105
#define TK_DELETE                         106
#define TK_UPDATE                         107
#define TK_SET                            108
#define TK_DEFERRABLE                     109
#define TK_FOREIGN                        110
#define TK_DROP                           111
#define TK_UNION                          112
#define TK_ALL                            113
#define TK_EXCEPT                         114
#define TK_INTERSECT                      115
#define TK_SELECT                         116
#define TK_DISTINCT                       117
#define TK_DOT                            118
#define TK_FROM                           119
#define TK_JOIN                           120
#define TK_USING                          121
#define TK_ORDER                          122
#define TK_GROUP                          123
#define TK_HAVING                         124
#define TK_LIMIT                          125
#define TK_WHERE                          126
#define TK_INTO                           127
#define TK_VALUES                         128
#define TK_INTEGER                        129
#define TK_FLOAT                          130
#define TK_BLOB                           131
#define TK_REGISTER                       132
#define TK_VARIABLE                       133
#define TK_CASE                           134
#define TK_WHEN                           135
#define TK_THEN                           136
#define TK_ELSE                           137
#define TK_INDEX                          138
#define TK_ALTER                          139
#define TK_ADD                            140
#define TK_TO_TEXT                        141
#define TK_TO_BLOB                        142
#define TK_TO_NUMERIC                     143
#define TK_TO_INT                         144
#define TK_TO_REAL                        145
#define TK_ISNOT                          146
#define TK_END_OF_FILE                    147
#define TK_ILLEGAL                        148
#define TK_SPACE                          149
#define TK_UNCLOSED_STRING                150
#define TK_FUNCTION                       151
#define TK_COLUMN                         152
#define TK_AGG_FUNCTION                   153
#define TK_AGG_COLUMN                     154
#define TK_CONST_FUNC                     155
#define TK_UMINUS                         156
#define TK_UPLUS                          157

/************** End of parse.h ***********************************************/
/************** Continuing where we left off in sqliteInt.h ******************/
//#include <stdio.h>
//#include <stdlib.h>
//#include <string.h>
//#include <assert.h>
//#include <stddef.h>

/*
** If compiling for a processor that lacks floating point support,
** substitute integer for floating-point
*/
#ifdef SQLITE_OMIT_FLOATING_POINT
# define double sqlite_int64
# define float sqlite_int64
# define LONGDOUBLE_TYPE sqlite_int64
# ifndef SQLITE_BIG_DBL
#   define SQLITE_BIG_DBL (((sqlite3_int64)1)<<50)
# endif
# define SQLITE_OMIT_DATETIME_FUNCS 1
# define SQLITE_OMIT_TRACE 1
# undef SQLITE_MIXED_ENDIAN_64BIT_FLOAT
# undef SQLITE_HAVE_ISNAN
#endif
#ifndef SQLITE_BIG_DBL
# define SQLITE_BIG_DBL (1e99)
#endif

/*
** OMIT_TEMPDB is set to 1 if SQLITE_OMIT_TEMPDB is defined, or 0
** afterward. Having this macro allows us to cause the C compiler 
** to omit code used by TEMP tables without messy #ifndef statements.
*/
#ifdef SQLITE_OMIT_TEMPDB
#define OMIT_TEMPDB 1
#else
#define OMIT_TEMPDB 0
#endif

/*
** The "file format" number is an integer that is incremented whenever
** the VDBE-level file format changes.  The following macros define the
** the default file format for new databases and the maximum file format
** that the library can read.
*/
#define SQLITE_MAX_FILE_FORMAT 4
#ifndef SQLITE_DEFAULT_FILE_FORMAT
# define SQLITE_DEFAULT_FILE_FORMAT 1
#endif

/*
** Determine whether triggers are recursive by default.  This can be
** changed at run-time using a pragma.
*/
#ifndef SQLITE_DEFAULT_RECURSIVE_TRIGGERS
# define SQLITE_DEFAULT_RECURSIVE_TRIGGERS 0
#endif

/*
** Provide a default value for SQLITE_TEMP_STORE in case it is not specified
** on the command-line
*/
#ifndef SQLITE_TEMP_STORE
# define SQLITE_TEMP_STORE 1
#endif

/*
** GCC does not define the OFFSETOF() macro so we'll have to do it
** ourselves.
*/
#ifndef OFFSETOF
#define OFFSETOF(STRUCTURE,FIELD) ((int)(long long)((char*)&((STRUCTURE*)0)->FIELD))
#endif

/*
** Check to see if this machine uses EBCDIC.  (Yes, believe it or
** not, there are still machines out there that use EBCDIC.)
*/
#if 'A' == '\301'
# define SQLITE_EBCDIC 1
#else
# define SQLITE_ASCII 1
#endif

/*
** Integers of known sizes.  These typedefs might change for architectures
** where the sizes very.  Preprocessor macros are available so that the
** types can be conveniently redefined at compile-type.  Like this:
**
**         cc '-DUINTPTR_TYPE=long long int' ...
*/
#ifndef UINT32_TYPE
# ifdef HAVE_UINT32_T
#  define UINT32_TYPE uint32_t
# else
#  define UINT32_TYPE unsigned int
# endif
#endif
#ifndef UINT16_TYPE
# ifdef HAVE_UINT16_T
#  define UINT16_TYPE uint16_t
# else
#  define UINT16_TYPE unsigned short int
# endif
#endif
#ifndef INT16_TYPE
# ifdef HAVE_INT16_T
#  define INT16_TYPE int16_t
# else
#  define INT16_TYPE short int
# endif
#endif
#ifndef UINT8_TYPE
# ifdef HAVE_UINT8_T
#  define UINT8_TYPE uint8_t
# else
#  define UINT8_TYPE unsigned char
# endif
#endif
#ifndef INT8_TYPE
# ifdef HAVE_INT8_T
#  define INT8_TYPE int8_t
# else
#  define INT8_TYPE signed char
# endif
#endif
#ifndef LONGDOUBLE_TYPE
# define LONGDOUBLE_TYPE long double
#endif

typedef int64_t  i64;          /* 8-byte signed integer */
typedef uint64_t u64;         /* 8-byte unsigned integer */
typedef uint32_t u32;           /* 4-byte unsigned integer */
typedef uint16_t u16;           /* 2-byte unsigned integer */
typedef int16_t  i16;            /* 2-byte signed integer */
typedef uint8_t  u8;             /* 1-byte unsigned integer */
typedef int8_t   i8;              /* 1-byte signed integer */

/*
** SQLITE_MAX_U32 is a u64 constant that is the maximum u64 value
** that can be stored in a u32 without loss of data.  The value
** is 0x00000000ffffffff.  But because of quirks of some compilers, we
** have to specify the value in the less intuitive manner shown:
*/
#define SQLITE_MAX_U32  ((((u64)1)<<32)-1)

/*
** Macros to determine whether the machine is big or little endian,
** evaluated at runtime.
*/
#ifdef SQLITE_AMALGAMATION
SQLITE_PRIVATE const int sqlite3one = 1;
#else
SQLITE_PRIVATE const int sqlite3one;
#endif
#if defined(i386) || defined(__i386__) || defined(_M_IX86)\
                             || defined(__x86_64) || defined(__x86_64__)
# define SQLITE_BIGENDIAN    0
# define SQLITE_LITTLEENDIAN 1
# define SQLITE_UTF16NATIVE  SQLITE_UTF16LE
#else
# define SQLITE_BIGENDIAN    (*(char *)(&sqlite3one)==0)
# define SQLITE_LITTLEENDIAN (*(char *)(&sqlite3one)==1)
# define SQLITE_UTF16NATIVE (SQLITE_BIGENDIAN?SQLITE_UTF16BE:SQLITE_UTF16LE)
#endif

/*
** Constants for the largest and smallest possible 64-bit signed integers.
** These macros are designed to work correctly on both 32-bit and 64-bit
** compilers.
*/
#define LARGEST_INT64  (0xffffffff|(((i64)0x7fffffff)<<32))
#define SMALLEST_INT64 (((i64)-1) - LARGEST_INT64)

/* 
** Round up a number to the next larger multiple of 8.  This is used
** to force 8-byte alignment on 64-bit architectures.
*/
#define ROUND8(x)     (((x)+7)&~7)

/*
** Round down to the nearest multiple of 8
*/
#define ROUNDDOWN8(x) ((x)&~7)

/*
** Assert that the pointer X is aligned to an 8-byte boundary.  This
** macro is used only within assert() to verify that the code gets
** all alignment restrictions correct.
**
** Except, if SQLITE_4_BYTE_ALIGNED_MALLOC is defined, then the
** underlying malloc() implemention might return us 4-byte aligned
** pointers.  In that case, only verify 4-byte alignment.
*/
#ifdef SQLITE_4_BYTE_ALIGNED_MALLOC
# define EIGHT_BYTE_ALIGNMENT(X)   ((((char*)(X) - (char*)0)&3)==0)
#else
# define EIGHT_BYTE_ALIGNMENT(X)   ((((char*)(X) - (char*)0)&7)==0)
#endif


/*
** An instance of the following structure is used to store the busy-handler
** callback for a given sqlite handle. 
**
** The sqlite.busyHandler member of the sqlite struct contains the busy
** callback for the database handle. Each pager opened via the sqlite
** handle is passed a pointer to sqlite.busyHandler. The busy-handler
** callback is currently invoked only from within pager.c.
*/
typedef struct BusyHandler BusyHandler;
struct BusyHandler {
  int (*xFunc)(void *,int);  /* The busy callback */
  void *pArg;                /* First arg to busy callback */
  int nBusy;                 /* Incremented with each busy call */
};

/*
** Name of the master database table.  The master database table
** is a special table that holds the names and attributes of all
** user tables and indices.
*/
#define MASTER_NAME       "sqlite_master"
#define TEMP_MASTER_NAME  "sqlite_temp_master"

/*
** The root-page of the master database table.
*/
#define MASTER_ROOT       1

/*
** The name of the schema table.
*/
#define SCHEMA_TABLE(x)  ((!OMIT_TEMPDB)&&(x==1)?TEMP_MASTER_NAME:MASTER_NAME)

/*
** A convenience macro that returns the number of elements in
** an array.
*/
#define ArraySize(X)    ((int)(sizeof(X)/sizeof(X[0])))

/*
** The following value as a destructor means to use sqlite3DbFree().
** This is an internal extension to SQLITE_STATIC and SQLITE_TRANSIENT.
*/
#define SQLITE_DYNAMIC   ((sqlite3_destructor_type)sqlite3DbFree)

/*
** When SQLITE_OMIT_WSD is defined, it means that the target platform does
** not support Writable Static Data (WSD) such as global and static variables.
** All variables must either be on the stack or dynamically allocated from
** the heap.  When WSD is unsupported, the variable declarations scattered
** throughout the SQLite code must become constants instead.  The SQLITE_WSD
** macro is used for this purpose.  And instead of referencing the variable
** directly, we use its constant as a key to lookup the run-time allocated
** buffer that holds real variable.  The constant is also the initializer
** for the run-time allocated buffer.
**
** In the usual case where WSD is supported, the SQLITE_WSD and GLOBAL
** macros become no-ops and have zero performance impact.
*/
#ifdef SQLITE_OMIT_WSD
  #define SQLITE_WSD const
  #define GLOBAL(t,v) (*(t*)sqlite3_wsd_find((void*)&(v), sizeof(v)))
  #define sqlite3GlobalConfig GLOBAL(struct Sqlite3Config, sqlite3Config)
SQLITE_API   int sqlite3_wsd_init(int N, int J);
SQLITE_API   void *sqlite3_wsd_find(void *K, int L);
#else
  #define SQLITE_WSD 
  #define GLOBAL(t,v) v
  #define sqlite3GlobalConfig sqlite3Config
#endif

/*
** The following macros are used to suppress compiler warnings and to
** make it clear to human readers when a function parameter is deliberately 
** left unused within the body of a function. This usually happens when
** a function is called via a function pointer. For example the 
** implementation of an SQL aggregate step callback may not use the
** parameter indicating the number of arguments passed to the aggregate,
** if it knows that this is enforced elsewhere.
**
** When a function parameter is not used at all within the body of a function,
** it is generally named "NotUsed" or "NotUsed2" to make things even clearer.
** However, these macros may also be used to suppress warnings related to
** parameters that may or may not be used depending on compilation options.
** For example those parameters only used in assert() statements. In these
** cases the parameters are named as per the usual conventions.
*/
#define UNUSED_PARAMETER(x) (void)(x)
#define UNUSED_PARAMETER2(x,y) UNUSED_PARAMETER(x),UNUSED_PARAMETER(y)

/*
** Forward references to structures
*/
typedef struct AggInfo AggInfo;
typedef struct AuthContext AuthContext;
typedef struct AutoincInfo AutoincInfo;
typedef struct Bitvec Bitvec;
typedef struct CollSeq CollSeq;
typedef struct Column Column;
typedef struct Db Db;
typedef struct Schema Schema;
typedef struct Expr Expr;
typedef struct ExprList ExprList;
typedef struct ExprSpan ExprSpan;
typedef struct FKey FKey;
typedef struct FuncDestructor FuncDestructor;
typedef struct FuncDef FuncDef;
typedef struct FuncDefHash FuncDefHash;
typedef struct IdList IdList;
typedef struct Index Index;
typedef struct IndexSample IndexSample;
typedef struct KeyClass KeyClass;
typedef struct KeyInfo KeyInfo;
typedef struct Lookaside Lookaside;
typedef struct LookasideSlot LookasideSlot;
typedef struct Module Module;
typedef struct NameContext NameContext;
typedef struct Parse Parse;
typedef struct RowSet RowSet;
typedef struct Savepoint Savepoint;
typedef struct Select Select;
typedef struct SrcList SrcList;
typedef struct StrAccum StrAccum;
typedef struct Table Table;
typedef struct TableLock TableLock;
typedef struct Token Token;
typedef struct Trigger Trigger;
typedef struct TriggerPrg TriggerPrg;
typedef struct TriggerStep TriggerStep;
typedef struct UnpackedRecord UnpackedRecord;
typedef struct VTable VTable;
typedef struct Walker Walker;
typedef struct WherePlan WherePlan;
typedef struct WhereInfo WhereInfo;
typedef struct WhereLevel WhereLevel;

/*
** Defer sourcing vdbe.h and btree.h until after the "u8" and 
** "BusyHandler" typedefs. vdbe.h also requires a few of the opaque
** pointer types (i.e. FuncDef) defined above.
*/
/************** Include btree.h in the middle of sqliteInt.h *****************/

#include "btree.h"

/************** Continuing where we left off in sqliteInt.h ******************/
/************** Include vdbe.h in the middle of sqliteInt.h ******************/
/************** Begin file vdbe.h ********************************************/
/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Header file for the Virtual DataBase Engine (VDBE)
**
** This header defines the interface to the virtual database engine
** or VDBE.  The VDBE implements an abstract machine that runs a
** simple program to access and modify the underlying database.
*/
#ifndef _SQLITE_VDBE_H_
#define _SQLITE_VDBE_H_

/*
** A single VDBE is an opaque structure named "Vdbe".  Only routines
** in the source file sqliteVdbe.c are allowed to see the insides
** of this structure.
*/
typedef struct Vdbe Vdbe;

/*
** The names of the following types declared in vdbeInt.h are required
** for the VdbeOp definition.
*/
typedef struct VdbeFunc VdbeFunc;
typedef struct Mem Mem;
typedef struct SubProgram SubProgram;

/*
** A single instruction of the virtual machine has an opcode
** and as many as three operands.  The instruction is recorded
** as an instance of the following structure:
*/
struct VdbeOp {
  u8 opcode;          /* What operation to perform */
  signed char p4type; /* One of the P4_xxx constants for p4 */
  u8 opflags;         /* Mask of the OPFLG_* flags in opcodes.h */
  u8 p5;              /* Fifth parameter is an unsigned character */
  i64 p1;             /* First operand */
  i64 p2;             /* Second parameter (often the jump destination) */
  i64 p3;             /* The third parameter */
  union {             /* fourth parameter */
    int i;                 /* Integer value if p4type==P4_INT32 */
    void *p;               /* Generic pointer */
    char *z;               /* Pointer to data for string (char array) types */
    i64 *pI64;             /* Used when p4type is P4_INT64 */
    double *pReal;         /* Used when p4type is P4_REAL */
    FuncDef *pFunc;        /* Used when p4type is P4_FUNCDEF */
    VdbeFunc *pVdbeFunc;   /* Used when p4type is P4_VDBEFUNC */
    CollSeq *pColl;        /* Used when p4type is P4_COLLSEQ */
    Mem *pMem;             /* Used when p4type is P4_MEM */
    VTable *pVtab;         /* Used when p4type is P4_VTAB */
    KeyInfo *pKeyInfo;     /* Used when p4type is P4_KEYINFO */
    int *ai;               /* Used when p4type is P4_INTARRAY */
    SubProgram *pProgram;  /* Used when p4type is P4_SUBPROGRAM */
  } p4;
#ifdef SQLITE_DEBUG
  char *zComment;          /* Comment to improve readability */
#endif
#ifdef VDBE_PROFILE
  int cnt;                 /* Number of times this instruction was executed */
  u64 cycles;              /* Total time spent executing this instruction */
#endif
};
typedef struct VdbeOp VdbeOp;


/*
** A sub-routine used to implement a trigger program.
*/
struct SubProgram {
  VdbeOp *aOp;                  /* Array of opcodes for sub-program */
  int nOp;                      /* Elements in aOp[] */
  int nMem;                     /* Number of memory cells required */
  int nCsr;                     /* Number of cursors required */
  void *token;                  /* id that may be used to recursive triggers */
  SubProgram *pNext;            /* Next sub-program already visited */
};

/*
** A smaller version of VdbeOp used for the VdbeAddOpList() function because
** it takes up less space.
*/
struct VdbeOpList {
  u8 opcode;          /* What operation to perform */
  signed char p1;     /* First operand */
  signed char p2;     /* Second parameter (often the jump destination) */
  signed char p3;     /* Third parameter */
};
typedef struct VdbeOpList VdbeOpList;

/*
** Allowed values of VdbeOp.p4type
*/
#define P4_NOTUSED    0   /* The P4 parameter is not used */
#define P4_DYNAMIC  (-1)  /* Pointer to a string obtained from sqliteMalloc() */
#define P4_STATIC   (-2)  /* Pointer to a static string */
#define P4_COLLSEQ  (-4)  /* P4 is a pointer to a CollSeq structure */
#define P4_FUNCDEF  (-5)  /* P4 is a pointer to a FuncDef structure */
#define P4_KEYINFO  (-6)  /* P4 is a pointer to a KeyInfo structure */
#define P4_VDBEFUNC (-7)  /* P4 is a pointer to a VdbeFunc structure */
#define P4_MEM      (-8)  /* P4 is a pointer to a Mem*    structure */
#define P4_TRANSIENT  0   /* P4 is a pointer to a transient string */
#define P4_VTAB     (-10) /* P4 is a pointer to an sqlite3_vtab structure */
#define P4_MPRINTF  (-11) /* P4 is a string obtained from sqlite3_mprintf() */
#define P4_REAL     (-12) /* P4 is a 64-bit floating point value */
#define P4_INT64    (-13) /* P4 is a 64-bit signed integer */
#define P4_INT32    (-14) /* P4 is a 32-bit signed integer */
#define P4_INTARRAY (-15) /* P4 is a vector of 32-bit integers */
#define P4_SUBPROGRAM  (-18) /* P4 is a pointer to a SubProgram structure */

/* When adding a P4 argument using P4_KEYINFO, a copy of the KeyInfo structure
** is made.  That copy is freed when the Vdbe is finalized.  But if the
** argument is P4_KEYINFO_HANDOFF, the passed in pointer is used.  It still
** gets freed when the Vdbe is finalized so it still should be obtained
** from a single sqliteMalloc().  But no copy is made and the calling
** function should *not* try to free the KeyInfo.
*/
#define P4_KEYINFO_HANDOFF (-16)
#define P4_KEYINFO_STATIC  (-17)

/*
** The Vdbe.aColName array contains 5n Mem structures, where n is the 
** number of columns of data returned by the statement.
*/
#define COLNAME_NAME     0
#define COLNAME_DECLTYPE 1
#define COLNAME_DATABASE 2
#define COLNAME_TABLE    3
#define COLNAME_COLUMN   4
#ifdef SQLITE_ENABLE_COLUMN_METADATA
# define COLNAME_N        5      /* Number of COLNAME_xxx symbols */
#else
# ifdef SQLITE_OMIT_DECLTYPE
#   define COLNAME_N      1      /* Store only the name */
# else
#   define COLNAME_N      2      /* Store the name and decltype */
# endif
#endif

/*
** The following macro converts a relative address in the p2 field
** of a VdbeOp structure into a negative number so that 
** sqlite3VdbeAddOpList() knows that the address is relative.  Calling
** the macro again restores the address.
*/
#define ADDR(X)  (-1-(X))

/*
** The makefile scans the vdbe.c source file and creates the "opcodes.h"
** header file that defines a number for each opcode used by the VDBE.
*/
/************** Include opcodes.h in the middle of vdbe.h ********************/
/************** Begin file opcodes.h *****************************************/
/* Automatically generated.  Do not edit */
/* See the mkopcodeh.awk script for details */
#define OP_Goto                                 1
#define OP_Gosub                                2
#define OP_Return                               3
#define OP_Yield                                4
#define OP_HaltIfNull                           5
#define OP_Halt                                 6
#define OP_Integer                              7
#define OP_Int64                                8
#define OP_Real                               130   /* same as TK_FLOAT    */
#define OP_String8                             94   /* same as TK_STRING   */
#define OP_String                               9
#define OP_Null                                10
#define OP_Blob                                11
#define OP_Variable                            12
#define OP_Move                                13
#define OP_Copy                                14
#define OP_SCopy                               15
#define OP_ResultRow                           16
#define OP_Concat                              91   /* same as TK_CONCAT   */
#define OP_Add                                 86   /* same as TK_PLUS     */
#define OP_Subtract                            87   /* same as TK_MINUS    */
#define OP_Multiply                            88   /* same as TK_STAR     */
#define OP_Divide                              89   /* same as TK_SLASH    */
#define OP_Remainder                           90   /* same as TK_REM      */
#define OP_CollSeq                             17
#define OP_Function                            18
#define OP_BitAnd                              82   /* same as TK_BITAND   */
#define OP_BitOr                               83   /* same as TK_BITOR    */
#define OP_ShiftLeft                           84   /* same as TK_LSHIFT   */
#define OP_ShiftRight                          85   /* same as TK_RSHIFT   */
#define OP_AddImm                              20
#define OP_MustBeInt                           21
#define OP_RealAffinity                        22
#define OP_ToText                             141   /* same as TK_TO_TEXT  */
#define OP_ToBlob                             142   /* same as TK_TO_BLOB  */
#define OP_ToNumeric                          143   /* same as TK_TO_NUMERIC*/
#define OP_ToInt                              144   /* same as TK_TO_INT   */
#define OP_ToReal                             145   /* same as TK_TO_REAL  */
#define OP_Eq                                  76   /* same as TK_EQ       */
#define OP_Ne                                  75   /* same as TK_NE       */
#define OP_Lt                                  79   /* same as TK_LT       */
#define OP_Le                                  78   /* same as TK_LE       */
#define OP_Gt                                  77   /* same as TK_GT       */
#define OP_Ge                                  80   /* same as TK_GE       */
#define OP_Permutation                         23
#define OP_Compare                             24
#define OP_Jump                                25
#define OP_And                                 69   /* same as TK_AND      */
#define OP_Or                                  68   /* same as TK_OR       */
#define OP_Not                                 19   /* same as TK_NOT      */
#define OP_BitNot                              93   /* same as TK_BITNOT   */
#define OP_If                                  26
#define OP_IfNot                               27
#define OP_IsNull                              73   /* same as TK_ISNULL   */
#define OP_NotNull                             74   /* same as TK_NOTNULL  */
#define OP_Column                              28
#define OP_Affinity                            29
#define OP_MakeRecord                          30
#define OP_Count                               31
#define OP_Savepoint                           32
#define OP_AutoCommit                          33
#define OP_Transaction                         34
#define OP_ReadCookie                          35
#define OP_SetCookie                           36
#define OP_VerifyCookie                        37
#define OP_OpenRead                            38
#define OP_OpenWrite                           39
#define OP_OpenAutoindex                       40
#define OP_OpenEphemeral                       41
#define OP_OpenPseudo                          42
#define OP_Close                               43
#define OP_SeekLt                              44
#define OP_SeekLe                              45
#define OP_SeekGe                              46
#define OP_SeekGt                              47
#define OP_Seek                                48
#define OP_NotFound                            49
#define OP_Found                               50
#define OP_IsUnique                            51
#define OP_NotExists                           52
#define OP_Sequence                            53
#define OP_NewRowid                            54
#define OP_Insert                              55
#define OP_InsertInt                           56
#define OP_Delete                              57
#define OP_ResetCount                          58
#define OP_RowKey                              59
#define OP_RowData                             60
#define OP_Rowid                               61
#define OP_NullRow                             62
#define OP_Last                                63
#define OP_Sort                                64
#define OP_Rewind                              65
#define OP_Prev                                66
#define OP_Next                                67
#define OP_IdxInsert                           70
#define OP_IdxDelete                           71
#define OP_IdxRowid                            72
#define OP_IdxLT                               81
#define OP_IdxGE                               92
#define OP_Destroy                             95
#define OP_Clear                               96
#define OP_CreateIndex                         97
#define OP_CreateTable                         98
#define OP_ParseSchema                         99
#define OP_LoadAnalysis                       100
#define OP_DropTable                          101
#define OP_DropIndex                          102
#define OP_DropTrigger                        103
#define OP_IntegrityCk                        104
#define OP_RowSetAdd                          105
#define OP_RowSetRead                         106
#define OP_RowSetTest                         107
#define OP_Program                            108
#define OP_Param                              109
#define OP_FkCounter                          110
#define OP_FkIfZero                           111
#define OP_MemMax                             112
#define OP_IfPos                              113
#define OP_IfNeg                              114
#define OP_IfZero                             115
#define OP_AggStep                            116
#define OP_AggFinal                           117
#define OP_Checkpoint                         118
#define OP_JournalMode                        119
#define OP_Vacuum                             120
#define OP_IncrVacuum                         121
#define OP_Expire                             122
#define OP_TableLock                          123
#define OP_VBegin                             124
#define OP_VCreate                            125
#define OP_VDestroy                           126
#define OP_VOpen                              127
#define OP_VFilter                            128
#define OP_VColumn                            129
#define OP_VNext                              131
#define OP_VRename                            132
#define OP_VUpdate                            133
#define OP_Pagecount                          134
#define OP_MaxPgcnt                           135
#define OP_Trace                              136
#define OP_Noop                               137
#define OP_Explain                            138

/* The following opcode values are never used */
#define OP_NotUsed_139                        139
#define OP_NotUsed_140                        140


/* Properties such as "out2" or "jump" that are specified in
** comments following the "case" for each opcode in the vdbe.c
** are encoded into bitvectors as follows:
*/
#define OPFLG_JUMP            0x0001  /* jump:  P2 holds jmp target */
#define OPFLG_OUT2_PRERELEASE 0x0002  /* out2-prerelease: */
#define OPFLG_IN1             0x0004  /* in1:   P1 is an input */
#define OPFLG_IN2             0x0008  /* in2:   P2 is an input */
#define OPFLG_IN3             0x0010  /* in3:   P3 is an input */
#define OPFLG_OUT2            0x0020  /* out2:  P2 is an output */
#define OPFLG_OUT3            0x0040  /* out3:  P3 is an output */
#define OPFLG_INITIALIZER {\
/*   0 */ 0x00, 0x01, 0x05, 0x04, 0x04, 0x10, 0x00, 0x02,\
/*   8 */ 0x02, 0x02, 0x02, 0x02, 0x02, 0x00, 0x24, 0x24,\
/*  16 */ 0x00, 0x00, 0x00, 0x24, 0x04, 0x05, 0x04, 0x00,\
/*  24 */ 0x00, 0x01, 0x05, 0x05, 0x00, 0x00, 0x00, 0x02,\
/*  32 */ 0x00, 0x00, 0x00, 0x02, 0x10, 0x00, 0x00, 0x00,\
/*  40 */ 0x00, 0x00, 0x00, 0x00, 0x11, 0x11, 0x11, 0x11,\
/*  48 */ 0x08, 0x11, 0x11, 0x11, 0x11, 0x02, 0x02, 0x00,\
/*  56 */ 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01,\
/*  64 */ 0x01, 0x01, 0x01, 0x01, 0x4c, 0x4c, 0x08, 0x00,\
/*  72 */ 0x02, 0x05, 0x05, 0x15, 0x15, 0x15, 0x15, 0x15,\
/*  80 */ 0x15, 0x01, 0x4c, 0x4c, 0x4c, 0x4c, 0x4c, 0x4c,\
/*  88 */ 0x4c, 0x4c, 0x4c, 0x4c, 0x01, 0x24, 0x02, 0x02,\
/*  96 */ 0x00, 0x02, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,\
/* 104 */ 0x00, 0x0c, 0x45, 0x15, 0x01, 0x02, 0x00, 0x01,\
/* 112 */ 0x08, 0x05, 0x05, 0x05, 0x00, 0x00, 0x00, 0x02,\
/* 120 */ 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,\
/* 128 */ 0x01, 0x00, 0x02, 0x01, 0x00, 0x00, 0x02, 0x02,\
/* 136 */ 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x04, 0x04,\
/* 144 */ 0x04, 0x04,}

/************** End of opcodes.h *********************************************/
/************** Continuing where we left off in vdbe.h ***********************/

/*
** Prototypes for the VDBE interface.  See comments on the implementation
** for a description of what each of these routines does.
*/
SQLITE_PRIVATE Vdbe *sqlite3VdbeCreate(sqlite3*);
SQLITE_PRIVATE int sqlite3VdbeAddOp0(Vdbe*,int);
SQLITE_PRIVATE int sqlite3VdbeAddOp1(Vdbe*,int,i64);
SQLITE_PRIVATE int sqlite3VdbeAddOp2(Vdbe*,int,i64,i64);
SQLITE_PRIVATE int sqlite3VdbeAddOp3(Vdbe*,int,i64,i64,i64);
SQLITE_PRIVATE int sqlite3VdbeAddOp4(Vdbe*,int,i64,i64,i64,const char *zP4,int);
SQLITE_PRIVATE int sqlite3VdbeAddOp4Int(Vdbe*,int,i64,i64,i64,int);
SQLITE_PRIVATE int sqlite3VdbeAddOpList(Vdbe*, int nOp, VdbeOpList const *aOp);
SQLITE_PRIVATE void sqlite3VdbeChangeP1(Vdbe*, int addr, i64 P1);
SQLITE_PRIVATE void sqlite3VdbeChangeP2(Vdbe*, int addr, i64 P2);
SQLITE_PRIVATE void sqlite3VdbeChangeP3(Vdbe*, int addr, i64 P3);
SQLITE_PRIVATE void sqlite3VdbeChangeP5(Vdbe*, u8 P5);
SQLITE_PRIVATE void sqlite3VdbeJumpHere(Vdbe*, int addr);
SQLITE_PRIVATE void sqlite3VdbeChangeToNoop(Vdbe*, int addr, int N);
SQLITE_PRIVATE void sqlite3VdbeChangeP4(Vdbe*, int addr, const char *zP4, int N);
SQLITE_PRIVATE void sqlite3VdbeUsesBtree(Vdbe*, int);
SQLITE_PRIVATE VdbeOp *sqlite3VdbeGetOp(Vdbe*, int);
SQLITE_PRIVATE int sqlite3VdbeMakeLabel(Vdbe*);
SQLITE_PRIVATE void sqlite3VdbeRunOnlyOnce(Vdbe*);
SQLITE_PRIVATE void sqlite3VdbeDelete(Vdbe*);
SQLITE_PRIVATE void sqlite3VdbeDeleteObject(sqlite3*,Vdbe*);
SQLITE_PRIVATE void sqlite3VdbeMakeReady(Vdbe*,int,int,int,int,int,int);
SQLITE_PRIVATE int sqlite3VdbeFinalize(Vdbe*);
SQLITE_PRIVATE void sqlite3VdbeResolveLabel(Vdbe*, int);
SQLITE_PRIVATE int sqlite3VdbeCurrentAddr(Vdbe*);
#if defined(SQLITE_DEBUG)
SQLITE_PRIVATE   int sqlite3VdbeAssertMayAbort(Vdbe *, int);
SQLITE_PRIVATE   void sqlite3VdbeTrace(Vdbe*,FILE*);
#endif
SQLITE_PRIVATE void sqlite3VdbeResetStepResult(Vdbe*);
SQLITE_PRIVATE int sqlite3VdbeReset(Vdbe*);
SQLITE_PRIVATE void sqlite3VdbeSetNumCols(Vdbe*,int);
SQLITE_PRIVATE int sqlite3VdbeSetColName(Vdbe*, int, int, const char *, void(*)(void*));
SQLITE_PRIVATE void sqlite3VdbeCountChanges(Vdbe*);
SQLITE_PRIVATE sqlite3 *sqlite3VdbeDb(Vdbe*);
SQLITE_PRIVATE void sqlite3VdbeSetSql(Vdbe*, const char *z, int n, int);
SQLITE_PRIVATE void sqlite3VdbeSwap(Vdbe*,Vdbe*);
SQLITE_PRIVATE VdbeOp *sqlite3VdbeTakeOpArray(Vdbe*, int*, int*);
SQLITE_PRIVATE sqlite3_value *sqlite3VdbeGetValue(Vdbe*, int, u8);
SQLITE_PRIVATE void sqlite3VdbeSetVarmask(Vdbe*, int);
#ifndef SQLITE_OMIT_TRACE
SQLITE_PRIVATE   char *sqlite3VdbeExpandSql(Vdbe*, const char*);
#endif

SQLITE_PRIVATE UnpackedRecord *sqlite3VdbeRecordUnpack(KeyInfo*,int,const void*,char*,int);
SQLITE_PRIVATE void sqlite3VdbeDeleteUnpackedRecord(UnpackedRecord*);
SQLITE_PRIVATE int sqlite3VdbeRecordCompare(int,const void*,UnpackedRecord*);

#ifndef SQLITE_OMIT_TRIGGER
SQLITE_PRIVATE void sqlite3VdbeLinkSubProgram(Vdbe *, SubProgram *);
#endif


#ifndef NDEBUG
SQLITE_PRIVATE   void sqlite3VdbeComment(Vdbe*, const char*, ...);
# define VdbeComment(X)  sqlite3VdbeComment X
SQLITE_PRIVATE   void sqlite3VdbeNoopComment(Vdbe*, const char*, ...);
# define VdbeNoopComment(X)  sqlite3VdbeNoopComment X
#else
# define VdbeComment(X)
# define VdbeNoopComment(X)
#endif

#endif

/************** End of vdbe.h ************************************************/
/************** Continuing where we left off in sqliteInt.h ******************/
/************** Include pager.h in the middle of sqliteInt.h *****************/
/************** Begin file pager.h *******************************************/
/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines the interface that the sqlite page cache
** subsystem.  The page cache subsystem reads and writes a file a page
** at a time and provides a journal for rollback.
*/

#ifndef _PAGER_H_
#define _PAGER_H_

/*
** Default maximum size for persistent journal files. A negative 
** value means no limit. This value may be overridden using the 
** sqlite3PagerJournalSizeLimit() API. See also "PRAGMA journal_size_limit".
*/
#ifndef SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT
  #define SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT -1
#endif

/*
** Each open file is managed by a separate instance of the "Pager" structure.
*/
typedef struct Pager Pager;

/*
** Handle type for pages.
*/
typedef struct PgHdr DbPage;

/*
** Page number PAGER_MJ_PGNO is never used in an SQLite database (it is
** reserved for working around a windows/posix incompatibility). It is
** used in the journal to signify that the remainder of the journal file 
** is devoted to storing a master journal name - there are no more pages to
** roll back. See comments for function writeMasterJournal() in pager.c 
** for details.
*/
#define PAGER_MJ_PGNO(x) ((Pgno)((PENDING_BYTE/((x)->pageSize))+1))

/*
** Allowed values for the flags parameter to sqlite3PagerOpen().
**
** NOTE: These values must match the corresponding BTREE_ values in btree.h.
*/
#define PAGER_OMIT_JOURNAL  0x0001    /* Do not use a rollback journal */
#define PAGER_NO_READLOCK   0x0002    /* Omit readlocks on readonly files */
#define PAGER_MEMORY        0x0004    /* In-memory database */

/*
** Valid values for the second argument to sqlite3PagerLockingMode().
*/
#define PAGER_LOCKINGMODE_QUERY      -1
#define PAGER_LOCKINGMODE_NORMAL      0
#define PAGER_LOCKINGMODE_EXCLUSIVE   1

/*
** Numeric constants that encode the journalmode.  
*/
#define PAGER_JOURNALMODE_QUERY     (-1)  /* Query the value of journalmode */
#define PAGER_JOURNALMODE_DELETE      0   /* Commit by deleting journal file */
#define PAGER_JOURNALMODE_PERSIST     1   /* Commit by zeroing journal header */
#define PAGER_JOURNALMODE_OFF         2   /* Journal omitted.  */
#define PAGER_JOURNALMODE_TRUNCATE    3   /* Commit by truncating journal */
#define PAGER_JOURNALMODE_MEMORY      4   /* In-memory journal file */
#define PAGER_JOURNALMODE_WAL         5   /* Use write-ahead logging */

/*
** The remainder of this file contains the declarations of the functions
** that make up the Pager sub-system API. See source code comments for 
** a detailed description of each routine.
*/

/* Open and close a Pager connection. */ 
SQLITE_PRIVATE int sqlite3PagerOpen(
  sqlite3_vfs*,
  Pager **ppPager,
  const char*,
  int,
  int,
  int,
  void(*)(DbPage*)
);
SQLITE_PRIVATE int sqlite3PagerClose(Pager *pPager);
SQLITE_PRIVATE int sqlite3PagerReadFileheader(Pager*, int, unsigned char*);

/* Functions used to configure a Pager object. */
SQLITE_PRIVATE void sqlite3PagerSetBusyhandler(Pager*, int(*)(void *), void *);
SQLITE_PRIVATE int sqlite3PagerSetPagesize(Pager*, u32*, int);
SQLITE_PRIVATE int sqlite3PagerMaxPageCount(Pager*, int);
SQLITE_PRIVATE void sqlite3PagerSetCachesize(Pager*, int);
SQLITE_PRIVATE void sqlite3PagerSetSafetyLevel(Pager*,int,int,int);
SQLITE_PRIVATE int sqlite3PagerLockingMode(Pager *, int);
SQLITE_PRIVATE int sqlite3PagerSetJournalMode(Pager *, int);
SQLITE_PRIVATE int sqlite3PagerGetJournalMode(Pager*);
SQLITE_PRIVATE int sqlite3PagerOkToChangeJournalMode(Pager*);
SQLITE_PRIVATE i64 sqlite3PagerJournalSizeLimit(Pager *, i64);
SQLITE_PRIVATE sqlite3_backup **sqlite3PagerBackupPtr(Pager*);

/* Functions used to obtain and release page references. */ 
SQLITE_PRIVATE int sqlite3PagerAcquire(Pager *pPager, Pgno pgno, DbPage **ppPage, int clrFlag);
#define sqlite3PagerGet(A,B,C) sqlite3PagerAcquire(A,B,C,0)
SQLITE_PRIVATE DbPage *sqlite3PagerLookup(Pager *pPager, Pgno pgno);
SQLITE_PRIVATE void sqlite3PagerRef(DbPage*);
SQLITE_PRIVATE void sqlite3PagerUnref(DbPage*);

/* Operations on page references. */
SQLITE_PRIVATE int sqlite3PagerWrite(DbPage*);
SQLITE_PRIVATE void sqlite3PagerDontWrite(DbPage*);
SQLITE_PRIVATE int sqlite3PagerMovepage(Pager*,DbPage*,Pgno,int);
SQLITE_PRIVATE int sqlite3PagerPageRefcount(DbPage*);
SQLITE_PRIVATE void *sqlite3PagerGetData(DbPage *); 
SQLITE_PRIVATE void *sqlite3PagerGetExtra(DbPage *); 

/* Functions used to manage pager transactions and savepoints. */
SQLITE_PRIVATE void sqlite3PagerPagecount(Pager*, int*);
SQLITE_PRIVATE int sqlite3PagerBegin(Pager*, int exFlag, int);
SQLITE_PRIVATE int sqlite3PagerCommitPhaseOne(Pager*,const char *zMaster, int);
SQLITE_PRIVATE int sqlite3PagerExclusiveLock(Pager*);
SQLITE_PRIVATE int sqlite3PagerSync(Pager *pPager);
SQLITE_PRIVATE int sqlite3PagerCommitPhaseTwo(Pager*);
SQLITE_PRIVATE int sqlite3PagerRollback(Pager*);
SQLITE_PRIVATE int sqlite3PagerOpenSavepoint(Pager *pPager, int n);
SQLITE_PRIVATE int sqlite3PagerSavepoint(Pager *pPager, int op, int iSavepoint);
SQLITE_PRIVATE int sqlite3PagerSharedLock(Pager *pPager);

SQLITE_PRIVATE int sqlite3PagerCheckpoint(Pager *pPager, int, int*, int*);
SQLITE_PRIVATE int sqlite3PagerWalSupported(Pager *pPager);
SQLITE_PRIVATE int sqlite3PagerWalCallback(Pager *pPager);
SQLITE_PRIVATE int sqlite3PagerOpenWal(Pager *pPager, int *pisOpen);
SQLITE_PRIVATE int sqlite3PagerCloseWal(Pager *pPager);

/* Functions used to query pager state and configuration. */
SQLITE_PRIVATE u8 sqlite3PagerIsreadonly(Pager*);
SQLITE_PRIVATE int sqlite3PagerRefcount(Pager*);
SQLITE_PRIVATE int sqlite3PagerMemUsed(Pager*);
SQLITE_PRIVATE const char *sqlite3PagerFilename(Pager*);
SQLITE_PRIVATE const sqlite3_vfs *sqlite3PagerVfs(Pager*);
SQLITE_PRIVATE sqlite3_file *sqlite3PagerFile(Pager*);
SQLITE_PRIVATE const char *sqlite3PagerJournalname(Pager*);
SQLITE_PRIVATE int sqlite3PagerNosync(Pager*);
SQLITE_PRIVATE void *sqlite3PagerTempSpace(Pager*);
SQLITE_PRIVATE int sqlite3PagerIsMemdb(Pager*);

/* Functions used to truncate the database file. */
SQLITE_PRIVATE void sqlite3PagerTruncateImage(Pager*,Pgno);

#if defined(SQLITE_HAS_CODEC) && !defined(SQLITE_OMIT_WAL)
SQLITE_PRIVATE void *sqlite3PagerCodec(DbPage *);
#endif

/* Functions to support testing and debugging. */
#if !defined(NDEBUG) || defined(SQLITE_TEST)
SQLITE_PRIVATE   Pgno sqlite3PagerPagenumber(DbPage*);
SQLITE_PRIVATE   int sqlite3PagerIswriteable(DbPage*);
#endif
#ifdef SQLITE_TEST
SQLITE_PRIVATE   int *sqlite3PagerStats(Pager*);
SQLITE_PRIVATE   void sqlite3PagerRefdump(Pager*);
  void disable_simulated_io_errors(void);
  void enable_simulated_io_errors(void);
#else
# define disable_simulated_io_errors()
# define enable_simulated_io_errors()
#endif

#endif /* _PAGER_H_ */

/************** End of pager.h ***********************************************/
/************** Continuing where we left off in sqliteInt.h ******************/
/************** Include pcache.h in the middle of sqliteInt.h ****************/
/************** Begin file pcache.h ******************************************/
/*
** 2008 August 05
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines the interface that the sqlite page cache
** subsystem. 
*/

#ifndef _PCACHE_H_

typedef struct PgHdr PgHdr;
typedef struct PCache PCache;

/*
** Every page in the cache is controlled by an instance of the following
** structure.
*/
struct PgHdr {
  void *pData;                   /* Content of this page */
  void *pExtra;                  /* Extra content */
  PgHdr *pDirty;                 /* Transient list of dirty pages */
  Pgno pgno;                     /* Page number for this page */
  Pager *pPager;                 /* The pager this page is part of */
#ifdef SQLITE_CHECK_PAGES
  u32 pageHash;                  /* Hash of page content */
#endif
  u16 flags;                     /* PGHDR flags defined below */

  /**********************************************************************
  ** Elements above are public.  All that follows is private to pcache.c
  ** and should not be accessed by other modules.
  */
  i16 nRef;                      /* Number of users of this page */
  PCache *pCache;                /* Cache that owns this page */

  PgHdr *pDirtyNext;             /* Next element in list of dirty pages */
  PgHdr *pDirtyPrev;             /* Previous element in list of dirty pages */
};

/* Bit values for PgHdr.flags */
#define PGHDR_DIRTY             0x002  /* Page has changed */
#define PGHDR_NEED_SYNC         0x004  /* Fsync the rollback journal before
                                       ** writing this page to the database */
#define PGHDR_NEED_READ         0x008  /* Content is unread */
#define PGHDR_REUSE_UNLIKELY    0x010  /* A hint that reuse is unlikely */
#define PGHDR_DONT_WRITE        0x020  /* Do not write content to disk */

/* Initialize and shutdown the page cache subsystem */
SQLITE_PRIVATE int sqlite3PcacheInitialize(void);
SQLITE_PRIVATE void sqlite3PcacheShutdown(void);

/* Page cache buffer management:
** These routines implement SQLITE_CONFIG_PAGECACHE.
*/
SQLITE_PRIVATE void sqlite3PCacheBufferSetup(void *, int sz, int n);

/* Create a new pager cache.
** Under memory stress, invoke xStress to try to make pages clean.
** Only clean and unpinned pages can be reclaimed.
*/
SQLITE_PRIVATE void sqlite3PcacheOpen(
  int szPage,                    /* Size of every page */
  int szExtra,                   /* Extra space associated with each page */
  int bPurgeable,                /* True if pages are on backing store */
  int (*xStress)(void*, PgHdr*), /* Call to try to make pages clean */
  void *pStress,                 /* Argument to xStress */
  PCache *pToInit                /* Preallocated space for the PCache */
);

/* Modify the page-size after the cache has been created. */
SQLITE_PRIVATE void sqlite3PcacheSetPageSize(PCache *, int);

/* Return the size in bytes of a PCache object.  Used to preallocate
** storage space.
*/
SQLITE_PRIVATE int sqlite3PcacheSize(void);

/* One release per successful fetch.  Page is pinned until released.
** Reference counted. 
*/
SQLITE_PRIVATE int sqlite3PcacheFetch(PCache*, Pgno, int createFlag, PgHdr**);
SQLITE_PRIVATE void sqlite3PcacheRelease(PgHdr*);

SQLITE_PRIVATE void sqlite3PcacheDrop(PgHdr*);         /* Remove page from cache */
SQLITE_PRIVATE void sqlite3PcacheMakeDirty(PgHdr*);    /* Make sure page is marked dirty */
SQLITE_PRIVATE void sqlite3PcacheMakeClean(PgHdr*);    /* Mark a single page as clean */
SQLITE_PRIVATE void sqlite3PcacheCleanAll(PCache*);    /* Mark all dirty list pages as clean */

/* Change a page number.  Used by incr-vacuum. */
SQLITE_PRIVATE void sqlite3PcacheMove(PgHdr*, Pgno);

/* Remove all pages with pgno>x.  Reset the cache if x==0 */
SQLITE_PRIVATE void sqlite3PcacheTruncate(PCache*, Pgno x);

/* Get a list of all dirty pages in the cache, sorted by page number */
SQLITE_PRIVATE PgHdr *sqlite3PcacheDirtyList(PCache*);

/* Reset and close the cache object */
SQLITE_PRIVATE void sqlite3PcacheClose(PCache*);

/* Clear flags from pages of the page cache */
SQLITE_PRIVATE void sqlite3PcacheClearSyncFlags(PCache *);

/* Discard the contents of the cache */
SQLITE_PRIVATE void sqlite3PcacheClear(PCache*);

/* Return the total number of outstanding page references */
SQLITE_PRIVATE int sqlite3PcacheRefCount(PCache*);

/* Increment the reference count of an existing page */
SQLITE_PRIVATE void sqlite3PcacheRef(PgHdr*);

SQLITE_PRIVATE int sqlite3PcachePageRefcount(PgHdr*);

/* Return the total number of pages stored in the cache */
SQLITE_PRIVATE int sqlite3PcachePagecount(PCache*);

#if defined(SQLITE_CHECK_PAGES) || defined(SQLITE_DEBUG)
/* Iterate through all dirty pages currently stored in the cache. This
** interface is only available if SQLITE_CHECK_PAGES is defined when the 
** library is built.
*/
SQLITE_PRIVATE void sqlite3PcacheIterateDirty(PCache *pCache, void (*xIter)(PgHdr *));
#endif

/* Set and get the suggested cache-size for the specified pager-cache.
**
** If no global maximum is configured, then the system attempts to limit
** the total number of pages cached by purgeable pager-caches to the sum
** of the suggested cache-sizes.
*/
SQLITE_PRIVATE void sqlite3PcacheSetCachesize(PCache *, int);
#ifdef SQLITE_TEST
SQLITE_PRIVATE int sqlite3PcacheGetCachesize(PCache *);
#endif

#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
/* Try to return memory used by the pcache module to the main memory heap */
SQLITE_PRIVATE int sqlite3PcacheReleaseMemory(int);
#endif

#ifdef SQLITE_TEST
SQLITE_PRIVATE void sqlite3PcacheStats(int*,int*,int*,int*);
#endif

SQLITE_PRIVATE void sqlite3PCacheSetDefault(void);

#endif /* _PCACHE_H_ */

/************** End of pcache.h **********************************************/
/************** Continuing where we left off in sqliteInt.h ******************/

/************** Include os.h in the middle of sqliteInt.h ********************/
/************** Begin file os.h **********************************************/
/*
** 2001 September 16
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This header file (together with is companion C source-code file
** "os.c") attempt to abstract the underlying operating system so that
** the SQLite library will work on both POSIX and windows systems.
**
** This header file is #include-ed by sqliteInt.h and thus ends up
** being included by every source file.
*/
#ifndef _SQLITE_OS_H_
#define _SQLITE_OS_H_

/*
** Figure out if we are dealing with Unix, Windows, or some other
** operating system.  After the following block of preprocess macros,
** all of SQLITE_OS_UNIX, SQLITE_OS_WIN, SQLITE_OS_OS2, and SQLITE_OS_OTHER 
** will defined to either 1 or 0.  One of the four will be 1.  The other 
** three will be 0.
*/
#if defined(SQLITE_OS_OTHER)
# if SQLITE_OS_OTHER==1
#   undef SQLITE_OS_UNIX
#   define SQLITE_OS_UNIX 0
#   undef SQLITE_OS_WIN
#   define SQLITE_OS_WIN 0
#   undef SQLITE_OS_OS2
#   define SQLITE_OS_OS2 0
# else
#   undef SQLITE_OS_OTHER
# endif
#endif
#if !defined(SQLITE_OS_UNIX) && !defined(SQLITE_OS_OTHER)
# define SQLITE_OS_OTHER 0
# ifndef SQLITE_OS_WIN
#   if defined(_WIN32) || defined(WIN32) || defined(__CYGWIN__) || defined(__MINGW32__) || defined(__BORLANDC__)
#     define SQLITE_OS_WIN 1
#     define SQLITE_OS_UNIX 0
#     define SQLITE_OS_OS2 0
#   elif defined(__EMX__) || defined(_OS2) || defined(OS2) || defined(_OS2_) || defined(__OS2__)
#     define SQLITE_OS_WIN 0
#     define SQLITE_OS_UNIX 0
#     define SQLITE_OS_OS2 1
#   else
#     define SQLITE_OS_WIN 0
#     define SQLITE_OS_UNIX 1
#     define SQLITE_OS_OS2 0
#  endif
# else
#  define SQLITE_OS_UNIX 0
#  define SQLITE_OS_OS2 0
# endif
#else
# ifndef SQLITE_OS_WIN
#  define SQLITE_OS_WIN 0
# endif
#endif

/*
** Determine if we are dealing with WindowsCE - which has a much
** reduced API.
*/
#if defined(_WIN32_WCE)
# define SQLITE_OS_WINCE 1
#else
# define SQLITE_OS_WINCE 0
#endif


/*
** Define the maximum size of a temporary filename
*/
#if SQLITE_OS_WIN
//# include <windows.h>
# define SQLITE_TEMPNAME_SIZE (MAX_PATH+50)
#elif SQLITE_OS_OS2
# if (__GNUC__ > 3 || __GNUC__ == 3 && __GNUC_MINOR__ >= 3) && defined(OS2_HIGH_MEMORY)
#  include <os2safe.h> /* has to be included before os2.h for linking to work */
# endif
# define INCL_DOSDATETIME
# define INCL_DOSFILEMGR
# define INCL_DOSERRORS
# define INCL_DOSMISC
# define INCL_DOSPROCESS
# define INCL_DOSMODULEMGR
# define INCL_DOSSEMAPHORES
//# include <os2.h>
//# include <uconv.h>
# define SQLITE_TEMPNAME_SIZE (CCHMAXPATHCOMP)
#else
# define SQLITE_TEMPNAME_SIZE 200
#endif

/* If the SET_FULLSYNC macro is not defined above, then make it
** a no-op
*/
#ifndef SET_FULLSYNC
# define SET_FULLSYNC(x,y)
#endif

/*
** The default size of a disk sector
*/
#ifndef SQLITE_DEFAULT_SECTOR_SIZE
# define SQLITE_DEFAULT_SECTOR_SIZE 512
#endif

/*
** Temporary files are named starting with this prefix followed by 16 random
** alphanumeric characters, and no file extension. They are stored in the
** OS's standard temporary file directory, and are deleted prior to exit.
** If sqlite is being embedded in another program, you may wish to change the
** prefix to reflect your program's name, so that if your program exits
** prematurely, old temporary files can be easily identified. This can be done
** using -DSQLITE_TEMP_FILE_PREFIX=myprefix_ on the compiler command line.
**
** 2006-10-31:  The default prefix used to be "sqlite_".  But then
** Mcafee started using SQLite in their anti-virus product and it
** started putting files with the "sqlite" name in the c:/temp folder.
** This annoyed many windows users.  Those users would then do a 
** Google search for "sqlite", find the telephone numbers of the
** developers and call to wake them up at night and complain.
** For this reason, the default name prefix is changed to be "sqlite" 
** spelled backwards.  So the temp files are still identified, but
** anybody smart enough to figure out the code is also likely smart
** enough to know that calling the developer will not help get rid
** of the file.
*/
#ifndef SQLITE_TEMP_FILE_PREFIX
# define SQLITE_TEMP_FILE_PREFIX "etilqs_"
#endif

/*
** The following values may be passed as the second argument to
** sqlite3OsLock(). The various locks exhibit the following semantics:
**
** SHARED:    Any number of processes may hold a SHARED lock simultaneously.
** RESERVED:  A single process may hold a RESERVED lock on a file at
**            any time. Other processes may hold and obtain new SHARED locks.
** PENDING:   A single process may hold a PENDING lock on a file at
**            any one time. Existing SHARED locks may persist, but no new
**            SHARED locks may be obtained by other processes.
** EXCLUSIVE: An EXCLUSIVE lock precludes all other locks.
**
** PENDING_LOCK may not be passed directly to sqlite3OsLock(). Instead, a
** process that requests an EXCLUSIVE lock may actually obtain a PENDING
** lock. This can be upgraded to an EXCLUSIVE lock by a subsequent call to
** sqlite3OsLock().
*/
#define NO_LOCK         0
#define SHARED_LOCK     1
#define RESERVED_LOCK   2
#define PENDING_LOCK    3
#define EXCLUSIVE_LOCK  4

/*
** File Locking Notes:  (Mostly about windows but also some info for Unix)
**
** We cannot use LockFileEx() or UnlockFileEx() on Win95/98/ME because
** those functions are not available.  So we use only LockFile() and
** UnlockFile().
**
** LockFile() prevents not just writing but also reading by other processes.
** A SHARED_LOCK is obtained by locking a single randomly-chosen 
** byte out of a specific range of bytes. The lock byte is obtained at 
** random so two separate readers can probably access the file at the 
** same time, unless they are unlucky and choose the same lock byte.
** An EXCLUSIVE_LOCK is obtained by locking all bytes in the range.
** There can only be one writer.  A RESERVED_LOCK is obtained by locking
** a single byte of the file that is designated as the reserved lock byte.
** A PENDING_LOCK is obtained by locking a designated byte different from
** the RESERVED_LOCK byte.
**
** On WinNT/2K/XP systems, LockFileEx() and UnlockFileEx() are available,
** which means we can use reader/writer locks.  When reader/writer locks
** are used, the lock is placed on the same range of bytes that is used
** for probabilistic locking in Win95/98/ME.  Hence, the locking scheme
** will support two or more Win95 readers or two or more WinNT readers.
** But a single Win95 reader will lock out all WinNT readers and a single
** WinNT reader will lock out all other Win95 readers.
**
** The following #defines specify the range of bytes used for locking.
** SHARED_SIZE is the number of bytes available in the pool from which
** a random byte is selected for a shared lock.  The pool of bytes for
** shared locks begins at SHARED_FIRST. 
**
** The same locking strategy and
** byte ranges are used for Unix.  This leaves open the possiblity of having
** clients on win95, winNT, and unix all talking to the same shared file
** and all locking correctly.  To do so would require that samba (or whatever
** tool is being used for file sharing) implements locks correctly between
** windows and unix.  I'm guessing that isn't likely to happen, but by
** using the same locking range we are at least open to the possibility.
**
** Locking in windows is manditory.  For this reason, we cannot store
** actual data in the bytes used for locking.  The pager never allocates
** the pages involved in locking therefore.  SHARED_SIZE is selected so
** that all locks will fit on a single page even at the minimum page size.
** PENDING_BYTE defines the beginning of the locks.  By default PENDING_BYTE
** is set high so that we don't have to allocate an unused page except
** for very large databases.  But one should test the page skipping logic 
** by setting PENDING_BYTE low and running the entire regression suite.
**
** Changing the value of PENDING_BYTE results in a subtly incompatible
** file format.  Depending on how it is changed, you might not notice
** the incompatibility right away, even running a full regression test.
** The default location of PENDING_BYTE is the first byte past the
** 1GB boundary.
**
*/
#ifdef SQLITE_OMIT_WSD
# define PENDING_BYTE     (0x40000000)
#else
# define PENDING_BYTE      sqlite3PendingByte
#endif
#define RESERVED_BYTE     (PENDING_BYTE+1)
#define SHARED_FIRST      (PENDING_BYTE+2)
#define SHARED_SIZE       510

/*
** Wrapper around OS specific sqlite3_os_init() function.
*/
SQLITE_PRIVATE int sqlite3OsInit(void);

/* 
** Functions for accessing sqlite3_file methods 
*/
SQLITE_PRIVATE int sqlite3OsClose(sqlite3_file*);
SQLITE_PRIVATE int sqlite3OsRead(sqlite3_file*, void*, int amt, i64 offset);
SQLITE_PRIVATE int sqlite3OsWrite(sqlite3_file*, const void*, int amt, i64 offset);
SQLITE_PRIVATE int sqlite3OsTruncate(sqlite3_file*, i64 size);
SQLITE_PRIVATE int sqlite3OsSync(sqlite3_file*, int);
SQLITE_PRIVATE int sqlite3OsFileSize(sqlite3_file*, i64 *pSize);
SQLITE_PRIVATE int sqlite3OsLock(sqlite3_file*, int);
SQLITE_PRIVATE int sqlite3OsUnlock(sqlite3_file*, int);
SQLITE_PRIVATE int sqlite3OsCheckReservedLock(sqlite3_file *id, int *pResOut);
SQLITE_PRIVATE int sqlite3OsFileControl(sqlite3_file*,int,void*);
#define SQLITE_FCNTL_DB_UNCHANGED 0xca093fa0
SQLITE_PRIVATE int sqlite3OsSectorSize(sqlite3_file *id);
SQLITE_PRIVATE int sqlite3OsDeviceCharacteristics(sqlite3_file *id);
SQLITE_PRIVATE int sqlite3OsShmMap(sqlite3_file *,int,int,int,void volatile **);
SQLITE_PRIVATE int sqlite3OsShmLock(sqlite3_file *id, int, int, int);
SQLITE_PRIVATE void sqlite3OsShmBarrier(sqlite3_file *id);
SQLITE_PRIVATE int sqlite3OsShmUnmap(sqlite3_file *id, int);

/* 
** Functions for accessing sqlite3_vfs methods 
*/
SQLITE_PRIVATE int sqlite3OsOpen(sqlite3_vfs *, const char *, sqlite3_file*, int, int *);
SQLITE_PRIVATE int sqlite3OsDelete(sqlite3_vfs *, const char *, int);
SQLITE_PRIVATE int sqlite3OsAccess(sqlite3_vfs *, const char *, int, int *pResOut);
SQLITE_PRIVATE int sqlite3OsFullPathname(sqlite3_vfs *, const char *, int, char *);
#ifndef SQLITE_OMIT_LOAD_EXTENSION
SQLITE_PRIVATE void *sqlite3OsDlOpen(sqlite3_vfs *, const char *);
SQLITE_PRIVATE void sqlite3OsDlError(sqlite3_vfs *, int, char *);
SQLITE_PRIVATE void (*sqlite3OsDlSym(sqlite3_vfs *, void *, const char *))(void);
SQLITE_PRIVATE void sqlite3OsDlClose(sqlite3_vfs *, void *);
#endif /* SQLITE_OMIT_LOAD_EXTENSION */
SQLITE_PRIVATE int sqlite3OsRandomness(sqlite3_vfs *, int, char *);
SQLITE_PRIVATE int sqlite3OsSleep(sqlite3_vfs *, int);
SQLITE_PRIVATE int sqlite3OsCurrentTimeInt64(sqlite3_vfs *, sqlite3_int64*);

/*
** Convenience functions for opening and closing files using 
** sqlite3_malloc() to obtain space for the file-handle structure.
*/
SQLITE_PRIVATE int sqlite3OsOpenMalloc(sqlite3_vfs *, const char *, sqlite3_file **, int,int*);
SQLITE_PRIVATE int sqlite3OsCloseFree(sqlite3_file *);

#endif /* _SQLITE_OS_H_ */

/************** End of os.h **************************************************/
/************** Continuing where we left off in sqliteInt.h ******************/
/************** Include mutex.h in the middle of sqliteInt.h *****************/
/************** Begin file mutex.h *******************************************/
/*
** 2007 August 28
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains the common header for all mutex implementations.
** The sqliteInt.h header #includes this file so that it is available
** to all source files.  We break it out in an effort to keep the code
** better organized.
**
** NOTE:  source files should *not* #include this header file directly.
** Source files should #include the sqliteInt.h file and let that file
** include this one indirectly.
*/


/*
** Figure out what version of the code to use.  The choices are
**
**   SQLITE_MUTEX_OMIT         No mutex logic.  Not even stubs.  The
**                             mutexes implemention cannot be overridden
**                             at start-time.
**
**   SQLITE_MUTEX_NOOP         For single-threaded applications.  No
**                             mutual exclusion is provided.  But this
**                             implementation can be overridden at
**                             start-time.
**
**   SQLITE_MUTEX_PTHREADS     For multi-threaded applications on Unix.
**
**   SQLITE_MUTEX_W32          For multi-threaded applications on Win32.
**
**   SQLITE_MUTEX_OS2          For multi-threaded applications on OS/2.
*/
#if !SQLITE_THREADSAFE
# define SQLITE_MUTEX_OMIT
#endif
#if SQLITE_THREADSAFE && !defined(SQLITE_MUTEX_NOOP)
#  if SQLITE_OS_UNIX
#    define SQLITE_MUTEX_PTHREADS
#  elif SQLITE_OS_WIN
#    define SQLITE_MUTEX_W32
#  elif SQLITE_OS_OS2
#    define SQLITE_MUTEX_OS2
#  else
#    define SQLITE_MUTEX_NOOP
#  endif
#endif

#ifdef SQLITE_MUTEX_OMIT
/*
** If this is a no-op implementation, implement everything as macros.
*/
#define sqlite3_mutex_alloc(X)    ((sqlite3_mutex*)8)
#define sqlite3_mutex_free(X)
#define sqlite3_mutex_enter(X)
#define sqlite3_mutex_try(X)      SQLITE_OK
#define sqlite3_mutex_leave(X)
#define sqlite3_mutex_held(X)     ((void)(X),1)
#define sqlite3_mutex_notheld(X)  ((void)(X),1)
#define sqlite3MutexAlloc(X)      ((sqlite3_mutex*)8)
#define sqlite3MutexInit()        SQLITE_OK
#define sqlite3MutexEnd()
#endif /* defined(SQLITE_MUTEX_OMIT) */

/************** End of mutex.h ***********************************************/
/************** Continuing where we left off in sqliteInt.h ******************/


/*
** Each database file to be accessed by the system is an instance
** of the following structure.  There are normally two of these structures
** in the sqlite.aDb[] array.  aDb[0] is the main database file and
** aDb[1] is the database file used to hold temporary tables.  Additional
** databases may be attached.
*/
struct Db {
  char *zName;         /* Name of this database */
  Btree *pBt;          /* The B*Tree structure for this database file */
  u8 inTrans;          /* 0: not writable.  1: Transaction.  2: Checkpoint */
  u8 safety_level;     /* How aggressive at syncing data to disk */
  Schema *pSchema;     /* Pointer to database schema (possibly shared) */
};

/*
** An instance of the following structure stores a database schema.
**
** Most Schema objects are associated with a Btree.  The exception is
** the Schema for the TEMP databaes (sqlite3.aDb[1]) which is free-standing.
** In shared cache mode, a single Schema object can be shared by multiple
** Btrees that refer to the same underlying BtShared object.
** 
** Schema objects are automatically deallocated when the last Btree that
** references them is destroyed.   The TEMP Schema is manually freed by
** sqlite3_close().
*
** A thread must be holding a mutex on the corresponding Btree in order
** to access Schema content.  This implies that the thread must also be
** holding a mutex on the sqlite3 connection pointer that owns the Btree.
** For a TEMP Schema, on the connection mutex is required.
*/
struct Schema {
  int schema_cookie;   /* Database schema version number for this file */
  int iGeneration;     /* Generation counter.  Incremented with each change */
  Hash tblHash;        /* All tables indexed by name */
  Hash idxHash;        /* All (named) indices indexed by name */
  Hash trigHash;       /* All triggers indexed by name */
  Hash fkeyHash;       /* All foreign keys by referenced table name */
  Table *pSeqTab;      /* The sqlite_sequence table used by AUTOINCREMENT */
  u8 file_format;      /* Schema format version for this file */
  u8 enc;              /* Text encoding used by this database */
  u16 flags;           /* Flags associated with this schema */
  int cache_size;      /* Number of pages to use in the cache */
};

/*
** These macros can be used to test, set, or clear bits in the 
** Db.pSchema->flags field.
*/
#define DbHasProperty(D,I,P)     (((D)->aDb[I].pSchema->flags&(P))==(P))
#define DbHasAnyProperty(D,I,P)  (((D)->aDb[I].pSchema->flags&(P))!=0)
#define DbSetProperty(D,I,P)     (D)->aDb[I].pSchema->flags|=(P)
#define DbClearProperty(D,I,P)   (D)->aDb[I].pSchema->flags&=~(P)

/*
** Allowed values for the DB.pSchema->flags field.
**
** The DB_SchemaLoaded flag is set after the database schema has been
** read into internal hash tables.
**
** DB_UnresetViews means that one or more views have column names that
** have been filled out.  If the schema changes, these column names might
** changes and so the view will need to be reset.
*/
#define DB_SchemaLoaded    0x0001  /* The schema has been loaded */
#define DB_UnresetViews    0x0002  /* Some views have defined column names */
#define DB_Empty           0x0004  /* The file is empty (length 0 bytes) */

/*
** The number of different kinds of things that can be limited
** using the sqlite3_limit() interface.
*/
#define SQLITE_N_LIMIT (SQLITE_LIMIT_TRIGGER_DEPTH+1)

/*
** Lookaside malloc is a set of fixed-size buffers that can be used
** to satisfy small transient memory allocation requests for objects
** associated with a particular database connection.  The use of
** lookaside malloc provides a significant performance enhancement
** (approx 10%) by avoiding numerous malloc/free requests while parsing
** SQL statements.
**
** The Lookaside structure holds configuration information about the
** lookaside malloc subsystem.  Each available memory allocation in
** the lookaside subsystem is stored on a linked list of LookasideSlot
** objects.
**
** Lookaside allocations are only allowed for objects that are associated
** with a particular database connection.  Hence, schema information cannot
** be stored in lookaside because in shared cache mode the schema information
** is shared by multiple database connections.  Therefore, while parsing
** schema information, the Lookaside.bEnabled flag is cleared so that
** lookaside allocations are not used to construct the schema objects.
*/
struct Lookaside {
  u16 sz;                 /* Size of each buffer in bytes */
  u8 bEnabled;            /* False to disable new lookaside allocations */
  u8 bMalloced;           /* True if pStart obtained from sqlite3_malloc() */
  int nOut;               /* Number of buffers currently checked out */
  int mxOut;              /* Highwater mark for nOut */
  int anStat[3];          /* 0: hits.  1: size misses.  2: full misses */
  LookasideSlot *pFree;   /* List of available buffers */
  void *pStart;           /* First byte of available memory space */
  void *pEnd;             /* First byte past end of available space */
};
struct LookasideSlot {
  LookasideSlot *pNext;    /* Next buffer in the list of free buffers */
};

/*
** A hash table for function definitions.
**
** Hash each FuncDef structure into one of the FuncDefHash.a[] slots.
** Collisions are on the FuncDef.pHash chain.
*/
struct FuncDefHash {
  FuncDef *a[23];       /* Hash table for functions */
};

/*
** Each database connection is an instance of the following structure.
**
** The sqlite.lastRowid records the last insert rowid generated by an
** insert statement.  Inserts on views do not affect its value.  Each
** trigger has its own context, so that lastRowid can be updated inside
** triggers as usual.  The previous value will be restored once the trigger
** exits.  Upon entering a before or instead of trigger, lastRowid is no
** longer (since after version 2.8.12) reset to -1.
**
** The sqlite.nChange does not count changes within triggers and keeps no
** context.  It is reset at start of sqlite3_exec.
** The sqlite.lsChange represents the number of changes made by the last
** insert, update, or delete statement.  It remains constant throughout the
** length of a statement and is then updated by OP_SetCounts.  It keeps a
** context stack just like lastRowid so that the count of changes
** within a trigger is not seen outside the trigger.  Changes to views do not
** affect the value of lsChange.
** The sqlite.csChange keeps track of the number of current changes (since
** the last statement) and is used to update sqlite_lsChange.
**
** The member variables sqlite.errCode, sqlite.zErrMsg and sqlite.zErrMsg16
** store the most recent error code and, if applicable, string. The
** internal function sqlite3Error() is used to set these variables
** consistently.
*/
struct sqlite3 {
  sqlite3_vfs *pVfs;            /* OS Interface */
  int nDb;                      /* Number of backends currently in use */
  Db *aDb;                      /* All backends */
  int flags;                    /* Miscellaneous flags. See below */
  int openFlags;                /* Flags passed to sqlite3_vfs.xOpen() */
  int errCode;                  /* Most recent error code (SQLITE_*) */
  int errMask;                  /* & result codes with this before returning */
  u8 autoCommit;                /* The auto-commit flag. */
  u8 temp_store;                /* 1: file 2: memory 0: default */
  u8 mallocFailed;              /* True if we have seen a malloc failure */
  u8 dfltLockMode;              /* Default locking-mode for attached dbs */
  signed char nextAutovac;      /* Autovac setting after VACUUM if >=0 */
  u8 suppressErr;               /* Do not issue error messages if true */
  int nextPagesize;             /* Pagesize after VACUUM if >0 */
  int nTable;                   /* Number of tables in the database */
  CollSeq *pDfltColl;           /* The default collating sequence (BINARY) */
  i64 lastRowid;                /* ROWID of most recent insert (see above) */
  u32 magic;                    /* Magic number for detect library misuse */
  int nChange;                  /* Value returned by sqlite3_changes() */
  int nTotalChange;             /* Value returned by sqlite3_total_changes() */
  sqlite3_mutex *mutex;         /* Connection mutex */
  int aLimit[SQLITE_N_LIMIT];   /* Limits */
  struct sqlite3InitInfo {      /* Information used during initialization */
    int iDb;                    /* When back is being initialized */
    Pgno newTnum;                /* Rootpage of table being initialized */
    u8 busy;                    /* TRUE if currently initializing */
    u8 orphanTrigger;           /* Last statement is orphaned TEMP trigger */
  } init;
  int nExtension;               /* Number of loaded extensions */
  void **aExtension;            /* Array of shared library handles */
  struct Vdbe *pVdbe;           /* List of active virtual machines */
  int activeVdbeCnt;            /* Number of VDBEs currently executing */
  int writeVdbeCnt;             /* Number of active VDBEs that are writing */
  int vdbeExecCnt;              /* Number of nested calls to VdbeExec() */
  void (*xTrace)(void*,const char*);        /* Trace function */
  void *pTraceArg;                          /* Argument to the trace function */
  void (*xProfile)(void*,const char*,u64);  /* Profiling function */
  void *pProfileArg;                        /* Argument to profile function */
  void *pCommitArg;                 /* Argument to xCommitCallback() */   
  int (*xCommitCallback)(void*);    /* Invoked at every commit. */
  void *pRollbackArg;               /* Argument to xRollbackCallback() */   
  void (*xRollbackCallback)(void*); /* Invoked at every commit. */
  void *pUpdateArg;
  void (*xUpdateCallback)(void*,int, const char*,const char*,sqlite_int64);
#ifndef SQLITE_OMIT_WAL
  int (*xWalCallback)(void *, sqlite3 *, const char *, int);
  void *pWalArg;
#endif
  void(*xCollNeeded)(void*,sqlite3*,int eTextRep,const char*);
  void(*xCollNeeded16)(void*,sqlite3*,int eTextRep,const void*);
  void *pCollNeededArg;
  sqlite3_value *pErr;          /* Most recent error message */
  char *zErrMsg;                /* Most recent error message (UTF-8 encoded) */
  char *zErrMsg16;              /* Most recent error message (UTF-16 encoded) */
  union {
    volatile int isInterrupted; /* True if sqlite3_interrupt has been called */
    double notUsed1;            /* Spacer */
  } u1;
  Lookaside lookaside;          /* Lookaside malloc configuration */
#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
  int (*xProgress)(void *);     /* The progress callback */
  void *pProgressArg;           /* Argument to the progress callback */
  int nProgressOps;             /* Number of opcodes for progress callback */
#endif
#ifndef SQLITE_OMIT_VIRTUALTABLE
  Hash aModule;                 /* populated by sqlite3_create_module() */
  Table *pVTab;                 /* vtab with active Connect/Create method */
  VTable **aVTrans;             /* Virtual tables with open transactions */
  int nVTrans;                  /* Allocated size of aVTrans */
  VTable *pDisconnect;    /* Disconnect these in next sqlite3_prepare() */
#endif
  FuncDefHash aFunc;            /* Hash table of connection functions */
  Hash aCollSeq;                /* All collating sequences */
  BusyHandler busyHandler;      /* Busy callback */
  int busyTimeout;              /* Busy handler timeout, in msec */
  Db aDbStatic[2];              /* Static space for the 2 default backends */
  Savepoint *pSavepoint;        /* List of active savepoints */
  int nSavepoint;               /* Number of non-transaction savepoints */
  int nStatement;               /* Number of nested statement-transactions  */
  u8 isTransactionSavepoint;    /* True if the outermost savepoint is a TS */
  i64 nDeferredCons;            /* Net deferred constraints this transaction. */
  int *pnBytesFreed;            /* If not NULL, increment this in DbFree() */

#ifdef SQLITE_ENABLE_UNLOCK_NOTIFY
  /* The following variables are all protected by the STATIC_MASTER 
  ** mutex, not by sqlite3.mutex. They are used by code in notify.c. 
  **
  ** When X.pUnlockConnection==Y, that means that X is waiting for Y to
  ** unlock so that it can proceed.
  **
  ** When X.pBlockingConnection==Y, that means that something that X tried
  ** tried to do recently failed with an SQLITE_LOCKED error due to locks
  ** held by Y.
  */
  sqlite3 *pBlockingConnection; /* Connection that caused SQLITE_LOCKED */
  sqlite3 *pUnlockConnection;           /* Connection to watch for unlock */
  void *pUnlockArg;                     /* Argument to xUnlockNotify */
  void (*xUnlockNotify)(void **, int);  /* Unlock notify callback */
  sqlite3 *pNextBlocked;        /* Next in list of all blocked connections */
#endif
};

/*
** A macro to discover the encoding of a database.
*/
#define ENC(db) ((db)->aDb[0].pSchema->enc)

/*
** Possible values for the sqlite3.flags.
*/
#define SQLITE_VdbeTrace      0x00000100  /* True to trace VDBE execution */
#define SQLITE_InternChanges  0x00000200  /* Uncommitted Hash table changes */
#define SQLITE_FullColNames   0x00000400  /* Show full column names on SELECT */
#define SQLITE_ShortColNames  0x00000800  /* Show short columns names */
#define SQLITE_CountRows      0x00001000  /* Count rows changed by INSERT, */
                                          /*   DELETE, or UPDATE and return */
                                          /*   the count using a callback. */
#define SQLITE_NullCallback   0x00002000  /* Invoke the callback once if the */
                                          /*   result set is empty */
#define SQLITE_SqlTrace       0x00004000  /* Debug print SQL as it executes */
#define SQLITE_VdbeListing    0x00008000  /* Debug listings of VDBE programs */
#define SQLITE_WriteSchema    0x00010000  /* OK to update SQLITE_MASTER */
#define SQLITE_NoReadlock     0x00020000  /* Readlocks are omitted when 
                                          ** accessing read-only databases */
#define SQLITE_IgnoreChecks   0x00040000  /* Do not enforce check constraints */
#define SQLITE_ReadUncommitted 0x0080000  /* For shared-cache mode */
#define SQLITE_LegacyFileFmt  0x00100000  /* Create new databases in format 1 */
#define SQLITE_FullFSync      0x00200000  /* Use full fsync on the backend */
#define SQLITE_CkptFullFSync  0x00400000  /* Use full fsync for checkpoint */
#define SQLITE_RecoveryMode   0x00800000  /* Ignore schema errors */
#define SQLITE_ReverseOrder   0x01000000  /* Reverse unordered SELECTs */
#define SQLITE_RecTriggers    0x02000000  /* Enable recursive triggers */
#define SQLITE_ForeignKeys    0x04000000  /* Enforce foreign key constraints  */
#define SQLITE_AutoIndex      0x08000000  /* Enable automatic indexes */
#define SQLITE_PreferBuiltin  0x10000000  /* Preference to built-in funcs */
#define SQLITE_LoadExtension  0x20000000  /* Enable load_extension */
#define SQLITE_EnableTrigger  0x40000000  /* True to enable triggers */

/*
** Bits of the sqlite3.flags field that are used by the
** sqlite3_test_control(SQLITE_TESTCTRL_OPTIMIZATIONS,...) interface.
** These must be the low-order bits of the flags field.
*/
#define SQLITE_QueryFlattener 0x01        /* Disable query flattening */
#define SQLITE_ColumnCache    0x02        /* Disable the column cache */
#define SQLITE_IndexSort      0x04        /* Disable indexes for sorting */
#define SQLITE_IndexSearch    0x08        /* Disable indexes for searching */
#define SQLITE_IndexCover     0x10        /* Disable index covering table */
#define SQLITE_GroupByOrder   0x20        /* Disable GROUPBY cover of ORDERBY */
#define SQLITE_FactorOutConst 0x40        /* Disable factoring out constants */
#define SQLITE_OptMask        0xff        /* Mask of all disablable opts */

/*
** Possible values for the sqlite.magic field.
** The numbers are obtained at random and have no special meaning, other
** than being distinct from one another.
*/
#define SQLITE_MAGIC_OPEN     0xa029a697  /* Database is open */
#define SQLITE_MAGIC_CLOSED   0x9f3c2d33  /* Database is closed */
#define SQLITE_MAGIC_SICK     0x4b771290  /* Error and awaiting close */
#define SQLITE_MAGIC_BUSY     0xf03b7906  /* Database currently in use */
#define SQLITE_MAGIC_ERROR    0xb5357930  /* An SQLITE_MISUSE error occurred */

/*
** Each SQL function is defined by an instance of the following
** structure.  A pointer to this structure is stored in the sqlite.aFunc
** hash table.  When multiple functions have the same name, the hash table
** points to a linked list of these structures.
*/
struct FuncDef {
  i16 nArg;            /* Number of arguments.  -1 means unlimited */
  u8 iPrefEnc;         /* Preferred text encoding (SQLITE_UTF8, 16LE, 16BE) */
  u8 flags;            /* Some combination of SQLITE_FUNC_* */
  void *pUserData;     /* User data parameter */
  FuncDef *pNext;      /* Next function with same name */
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**); /* Regular function */
  void (*xStep)(sqlite3_context*,int,sqlite3_value**); /* Aggregate step */
  void (*xFinalize)(sqlite3_context*);                /* Aggregate finalizer */
  char *zName;         /* SQL name of the function. */
  FuncDef *pHash;      /* Next with a different name but the same hash */
  FuncDestructor *pDestructor;   /* Reference counted destructor function */
};

/*
** This structure encapsulates a user-function destructor callback (as
** configured using create_function_v2()) and a reference counter. When
** create_function_v2() is called to create a function with a destructor,
** a single object of this type is allocated. FuncDestructor.nRef is set to 
** the number of FuncDef objects created (either 1 or 3, depending on whether
** or not the specified encoding is SQLITE_ANY). The FuncDef.pDestructor
** member of each of the new FuncDef objects is set to point to the allocated
** FuncDestructor.
**
** Thereafter, when one of the FuncDef objects is deleted, the reference
** count on this object is decremented. When it reaches 0, the destructor
** is invoked and the FuncDestructor structure freed.
*/
struct FuncDestructor {
  int nRef;
  void (*xDestroy)(void *);
  void *pUserData;
};

/*
** Possible values for FuncDef.flags
*/
#define SQLITE_FUNC_LIKE     0x01 /* Candidate for the LIKE optimization */
#define SQLITE_FUNC_CASE     0x02 /* Case-sensitive LIKE-type function */
#define SQLITE_FUNC_EPHEM    0x04 /* Ephemeral.  Delete with VDBE */
#define SQLITE_FUNC_NEEDCOLL 0x08 /* sqlite3GetFuncCollSeq() might be called */
#define SQLITE_FUNC_PRIVATE  0x10 /* Allowed for internal use only */
#define SQLITE_FUNC_COUNT    0x20 /* Built-in count(*) aggregate */
#define SQLITE_FUNC_COALESCE 0x40 /* Built-in coalesce() or ifnull() function */

/*
** The following three macros, FUNCTION(), LIKEFUNC() and AGGREGATE() are
** used to create the initializers for the FuncDef structures.
**
**   FUNCTION(zName, nArg, iArg, bNC, xFunc)
**     Used to create a scalar function definition of a function zName 
**     implemented by C function xFunc that accepts nArg arguments. The
**     value passed as iArg is cast to a (void*) and made available
**     as the user-data (sqlite3_user_data()) for the function. If 
**     argument bNC is true, then the SQLITE_FUNC_NEEDCOLL flag is set.
**
**   AGGREGATE(zName, nArg, iArg, bNC, xStep, xFinal)
**     Used to create an aggregate function definition implemented by
**     the C functions xStep and xFinal. The first four parameters
**     are interpreted in the same way as the first 4 parameters to
**     FUNCTION().
**
**   LIKEFUNC(zName, nArg, pArg, flags)
**     Used to create a scalar function definition of a function zName 
**     that accepts nArg arguments and is implemented by a call to C 
**     function likeFunc. Argument pArg is cast to a (void *) and made
**     available as the function user-data (sqlite3_user_data()). The
**     FuncDef.flags variable is set to the value passed as the flags
**     parameter.
*/
#define FUNCTION(zName, nArg, iArg, bNC, xFunc) \
  {nArg, SQLITE_UTF8, bNC*SQLITE_FUNC_NEEDCOLL, \
   SQLITE_INT_TO_PTR(iArg), 0, xFunc, 0, 0, #zName, 0, 0}
#define STR_FUNCTION(zName, nArg, pArg, bNC, xFunc) \
  {nArg, SQLITE_UTF8, bNC*SQLITE_FUNC_NEEDCOLL, \
   pArg, 0, xFunc, 0, 0, #zName, 0, 0}
#define LIKEFUNC(zName, nArg, arg, flags) \
  {nArg, SQLITE_UTF8, flags, (void *)arg, 0, likeFunc, 0, 0, #zName, 0, 0}
#define AGGREGATE(zName, nArg, arg, nc, xStep, xFinal) \
  {nArg, SQLITE_UTF8, nc*SQLITE_FUNC_NEEDCOLL, \
   SQLITE_INT_TO_PTR(arg), 0, 0, xStep,xFinal,#zName,0,0}

/*
** All current savepoints are stored in a linked list starting at
** sqlite3.pSavepoint. The first element in the list is the most recently
** opened savepoint. Savepoints are added to the list by the vdbe
** OP_Savepoint instruction.
*/
struct Savepoint {
  char *zName;                        /* Savepoint name (nul-terminated) */
  i64 nDeferredCons;                  /* Number of deferred fk violations */
  Savepoint *pNext;                   /* Parent savepoint (if any) */
};

/*
** The following are used as the second parameter to sqlite3Savepoint(),
** and as the P1 argument to the OP_Savepoint instruction.
*/
#define SAVEPOINT_BEGIN      0
#define SAVEPOINT_RELEASE    1
#define SAVEPOINT_ROLLBACK   2


/*
** Each SQLite module (virtual table definition) is defined by an
** instance of the following structure, stored in the sqlite3.aModule
** hash table.
*/
struct Module {
  const sqlite3_module *pModule;       /* Callback pointers */
  const char *zName;                   /* Name passed to create_module() */
  void *pAux;                          /* pAux passed to create_module() */
  void (*xDestroy)(void *);            /* Module destructor function */
};

/*
** information about each column of an SQL table is held in an instance
** of this structure.
*/
struct Column {
  char *zName;     /* Name of this column */
  Expr *pDflt;     /* Default value of this column */
  char *zDflt;     /* Original text of the default value */
  char *zType;     /* Data type for this column */
  char *zColl;     /* Collating sequence.  If NULL, use the default */
  u8 notNull;      /* True if there is a NOT NULL constraint */
  u8 isPrimKey;    /* True if this column is part of the PRIMARY KEY */
  char affinity;   /* One of the SQLITE_AFF_... values */
#ifndef SQLITE_OMIT_VIRTUALTABLE
  u8 isHidden;     /* True if this column is 'hidden' */
#endif
};

/*
** A "Collating Sequence" is defined by an instance of the following
** structure. Conceptually, a collating sequence consists of a name and
** a comparison routine that defines the order of that sequence.
**
** There may two separate implementations of the collation function, one
** that processes text in UTF-8 encoding (CollSeq.xCmp) and another that
** processes text encoded in UTF-16 (CollSeq.xCmp16), using the machine
** native byte order. When a collation sequence is invoked, SQLite selects
** the version that will require the least expensive encoding
** translations, if any.
**
** The CollSeq.pUser member variable is an extra parameter that passed in
** as the first argument to the UTF-8 comparison function, xCmp.
** CollSeq.pUser16 is the equivalent for the UTF-16 comparison function,
** xCmp16.
**
** If both CollSeq.xCmp and CollSeq.xCmp16 are NULL, it means that the
** collating sequence is undefined.  Indices built on an undefined
** collating sequence may not be read or written.
*/
struct CollSeq {
  char *zName;          /* Name of the collating sequence, UTF-8 encoded */
  u8 enc;               /* Text encoding handled by xCmp() */
  u8 type;              /* One of the SQLITE_COLL_... values below */
  void *pUser;          /* First argument to xCmp() */
  int (*xCmp)(void*,int, const void*, int, const void*);
  void (*xDel)(void*);  /* Destructor for pUser */
};

/*
** Allowed values of CollSeq.type:
*/
#define SQLITE_COLL_BINARY  1  /* The default memcmp() collating sequence */
#define SQLITE_COLL_NOCASE  2  /* The built-in NOCASE collating sequence */
#define SQLITE_COLL_REVERSE 3  /* The built-in REVERSE collating sequence */
#define SQLITE_COLL_USER    0  /* Any other user-defined collating sequence */

/*
** A sort order can be either ASC or DESC.
*/
#define SQLITE_SO_ASC       0  /* Sort in ascending order */
#define SQLITE_SO_DESC      1  /* Sort in ascending order */

/*
** Column affinity types.
**
** These used to have mnemonic name like 'i' for SQLITE_AFF_INTEGER and
** 't' for SQLITE_AFF_TEXT.  But we can save a little space and improve
** the speed a little by numbering the values consecutively.  
**
** But rather than start with 0 or 1, we begin with 'a'.  That way,
** when multiple affinity types are concatenated into a string and
** used as the P4 operand, they will be more readable.
**
** Note also that the numeric types are grouped together so that testing
** for a numeric type is a single comparison.
*/
#define SQLITE_AFF_TEXT     'a'
#define SQLITE_AFF_NONE     'b'
#define SQLITE_AFF_NUMERIC  'c'
#define SQLITE_AFF_INTEGER  'd'
#define SQLITE_AFF_REAL     'e'

#define sqlite3IsNumericAffinity(X)  ((X)>=SQLITE_AFF_NUMERIC)

/*
** The SQLITE_AFF_MASK values masks off the significant bits of an
** affinity value. 
*/
#define SQLITE_AFF_MASK     0x67

/*
** Additional bit values that can be ORed with an affinity without
** changing the affinity.
*/
#define SQLITE_JUMPIFNULL   0x08  /* jumps if either operand is NULL */
#define SQLITE_STOREP2      0x10  /* Store result in reg[P2] rather than jump */
#define SQLITE_NULLEQ       0x80  /* NULL=NULL */

/*
** An object of this type is created for each virtual table present in
** the database schema. 
**
** If the database schema is shared, then there is one instance of this
** structure for each database connection (sqlite3*) that uses the shared
** schema. This is because each database connection requires its own unique
** instance of the sqlite3_vtab* handle used to access the virtual table 
** implementation. sqlite3_vtab* handles can not be shared between 
** database connections, even when the rest of the in-memory database 
** schema is shared, as the implementation often stores the database
** connection handle passed to it via the xConnect() or xCreate() method
** during initialization internally. This database connection handle may
** then be used by the virtual table implementation to access real tables 
** within the database. So that they appear as part of the callers 
** transaction, these accesses need to be made via the same database 
** connection as that used to execute SQL operations on the virtual table.
**
** All VTable objects that correspond to a single table in a shared
** database schema are initially stored in a linked-list pointed to by
** the Table.pVTable member variable of the corresponding Table object.
** When an sqlite3_prepare() operation is required to access the virtual
** table, it searches the list for the VTable that corresponds to the
** database connection doing the preparing so as to use the correct
** sqlite3_vtab* handle in the compiled query.
**
** When an in-memory Table object is deleted (for example when the
** schema is being reloaded for some reason), the VTable objects are not 
** deleted and the sqlite3_vtab* handles are not xDisconnect()ed 
** immediately. Instead, they are moved from the Table.pVTable list to
** another linked list headed by the sqlite3.pDisconnect member of the
** corresponding sqlite3 structure. They are then deleted/xDisconnected 
** next time a statement is prepared using said sqlite3*. This is done
** to avoid deadlock issues involving multiple sqlite3.mutex mutexes.
** Refer to comments above function sqlite3VtabUnlockList() for an
** explanation as to why it is safe to add an entry to an sqlite3.pDisconnect
** list without holding the corresponding sqlite3.mutex mutex.
**
** The memory for objects of this type is always allocated by 
** sqlite3DbMalloc(), using the connection handle stored in VTable.db as 
** the first argument.
*/
struct VTable {
  sqlite3 *db;              /* Database connection associated with this table */
  Module *pMod;             /* Pointer to module implementation */
  sqlite3_vtab *pVtab;      /* Pointer to vtab instance */
  int nRef;                 /* Number of pointers to this structure */
  VTable *pNext;            /* Next in linked list (see above) */
};

/*
** Each SQL table is represented in memory by an instance of the
** following structure.
**
** Table.zName is the name of the table.  The case of the original
** CREATE TABLE statement is stored, but case is not significant for
** comparisons.
**
** Table.nCol is the number of columns in this table.  Table.aCol is a
** pointer to an array of Column structures, one for each column.
**
** If the table has an INTEGER PRIMARY KEY, then Table.iPKey is the index of
** the column that is that key.   Otherwise Table.iPKey is negative.  Note
** that the datatype of the PRIMARY KEY must be INTEGER for this field to
** be set.  An INTEGER PRIMARY KEY is used as the rowid for each row of
** the table.  If a table has no INTEGER PRIMARY KEY, then a random rowid
** is generated for each row of the table.  TF_HasPrimaryKey is set if
** the table has any PRIMARY KEY, INTEGER or otherwise.
**
** Table.tnum is the page number for the root BTree page of the table in the
** database file.  If Table.iDb is the index of the database table backend
** in sqlite.aDb[].  0 is for the main database and 1 is for the file that
** holds temporary tables and indices.  If TF_Ephemeral is set
** then the table is stored in a file that is automatically deleted
** when the VDBE cursor to the table is closed.  In this case Table.tnum 
** refers VDBE cursor number that holds the table open, not to the root
** page number.  Transient tables are used to hold the results of a
** sub-query that appears instead of a real table name in the FROM clause 
** of a SELECT statement.
*/
struct Table {
  char *zName;         /* Name of the table or view */
  int iPKey;           /* If not negative, use aCol[iPKey] as the primary key */
  int nCol;            /* Number of columns in this table */
  Column *aCol;        /* Information about each column */
  Index *pIndex;       /* List of SQL indexes on this table. */
  Pgno tnum;            /* Root BTree node for this table (see note above) */
  unsigned nRowEst;    /* Estimated rows in table - from sqlite_stat1 table */
  Select *pSelect;     /* NULL for tables.  Points to definition if a view. */
  u16 nRef;            /* Number of pointers to this Table */
  u8 tabFlags;         /* Mask of TF_* values */
  u8 keyConf;          /* What to do in case of uniqueness conflict on iPKey */
  FKey *pFKey;         /* Linked list of all foreign keys in this table */
  char *zColAff;       /* String defining the affinity of each column */
#ifndef SQLITE_OMIT_CHECK
  Expr *pCheck;        /* The AND of all CHECK constraints */
#endif
#ifndef SQLITE_OMIT_ALTERTABLE
  int addColOffset;    /* Offset in CREATE TABLE stmt to add a new column */
#endif
#ifndef SQLITE_OMIT_VIRTUALTABLE
  VTable *pVTable;     /* List of VTable objects. */
  int nModuleArg;      /* Number of arguments to the module */
  char **azModuleArg;  /* Text of all module args. [0] is module name */
#endif
  Trigger *pTrigger;   /* List of triggers stored in pSchema */
  Schema *pSchema;     /* Schema that contains this table */
  Table *pNextZombie;  /* Next on the Parse.pZombieTab list */
};

/*
** Allowed values for Tabe.tabFlags.
*/
#define TF_Readonly        0x01    /* Read-only system table */
#define TF_Ephemeral       0x02    /* An ephemeral table */
#define TF_HasPrimaryKey   0x04    /* Table has a primary key */
#define TF_Autoincrement   0x08    /* Integer primary key is autoincrement */
#define TF_Virtual         0x10    /* Is a virtual table */
#define TF_NeedMetadata    0x20    /* aCol[].zType and aCol[].pColl missing */



/*
** Test to see whether or not a table is a virtual table.  This is
** done as a macro so that it will be optimized out when virtual
** table support is omitted from the build.
*/
#ifndef SQLITE_OMIT_VIRTUALTABLE
#  define IsVirtual(X)      (((X)->tabFlags & TF_Virtual)!=0)
#  define IsHiddenColumn(X) ((X)->isHidden)
#else
#  define IsVirtual(X)      0
#  define IsHiddenColumn(X) 0
#endif

/*
** Each foreign key constraint is an instance of the following structure.
**
** A foreign key is associated with two tables.  The "from" table is
** the table that contains the REFERENCES clause that creates the foreign
** key.  The "to" table is the table that is named in the REFERENCES clause.
** Consider this example:
**
**     CREATE TABLE ex1(
**       a INTEGER PRIMARY KEY,
**       b INTEGER CONSTRAINT fk1 REFERENCES ex2(x)
**     );
**
** For foreign key "fk1", the from-table is "ex1" and the to-table is "ex2".
**
** Each REFERENCES clause generates an instance of the following structure
** which is attached to the from-table.  The to-table need not exist when
** the from-table is created.  The existence of the to-table is not checked.
*/
struct FKey {
  Table *pFrom;     /* Table containing the REFERENCES clause (aka: Child) */
  FKey *pNextFrom;  /* Next foreign key in pFrom */
  char *zTo;        /* Name of table that the key points to (aka: Parent) */
  FKey *pNextTo;    /* Next foreign key on table named zTo */
  FKey *pPrevTo;    /* Previous foreign key on table named zTo */
  int nCol;         /* Number of columns in this key */
  /* EV: R-30323-21917 */
  u8 isDeferred;    /* True if constraint checking is deferred till COMMIT */
  u8 aAction[2];          /* ON DELETE and ON UPDATE actions, respectively */
  Trigger *apTrigger[2];  /* Triggers for aAction[] actions */
  struct sColMap {  /* Mapping of columns in pFrom to columns in zTo */
    int iFrom;         /* Index of column in pFrom */
    char *zCol;        /* Name of column in zTo.  If 0 use PRIMARY KEY */
  } aCol[1];        /* One entry for each of nCol column s */
};

/*
** SQLite supports many different ways to resolve a constraint
** error.  ROLLBACK processing means that a constraint violation
** causes the operation in process to fail and for the current transaction
** to be rolled back.  ABORT processing means the operation in process
** fails and any prior changes from that one operation are backed out,
** but the transaction is not rolled back.  FAIL processing means that
** the operation in progress stops and returns an error code.  But prior
** changes due to the same operation are not backed out and no rollback
** occurs.  IGNORE means that the particular row that caused the constraint
** error is not inserted or updated.  Processing continues and no error
** is returned.  REPLACE means that preexisting database rows that caused
** a UNIQUE constraint violation are removed so that the new insert or
** update can proceed.  Processing continues and no error is reported.
**
** RESTRICT, SETNULL, and CASCADE actions apply only to foreign keys.
** RESTRICT is the same as ABORT for IMMEDIATE foreign keys and the
** same as ROLLBACK for DEFERRED keys.  SETNULL means that the foreign
** key is set to NULL.  CASCADE means that a DELETE or UPDATE of the
** referenced table row is propagated into the row that holds the
** foreign key.
** 
** The following symbolic values are used to record which type
** of action to take.
*/
#define OE_None     0   /* There is no constraint to check */
#define OE_Rollback 1   /* Fail the operation and rollback the transaction */
#define OE_Abort    2   /* Back out changes but do no rollback transaction */
#define OE_Fail     3   /* Stop the operation but leave all prior changes */
#define OE_Ignore   4   /* Ignore the error. Do not do the INSERT or UPDATE */
#define OE_Replace  5   /* Delete existing record, then do INSERT or UPDATE */

#define OE_Restrict 6   /* OE_Abort for IMMEDIATE, OE_Rollback for DEFERRED */
#define OE_SetNull  7   /* Set the foreign key value to NULL */
#define OE_SetDflt  8   /* Set the foreign key value to its default */
#define OE_Cascade  9   /* Cascade the changes */

#define OE_Default  99  /* Do whatever the default action is */


/*
** An instance of the following structure is passed as the first
** argument to sqlite3VdbeKeyCompare and is used to control the 
** comparison of the two index keys.
*/
struct KeyInfo {
  int refcount;       // YESQUEL CH: added refcount for smart pointers
                      //   We use smart pointers only within Yesquel code
                      //   not sqlite. Sqlite's use of a keyinfo counts
                      //   as a single refcount, set when the keyinfo is
                      //   created using our newly added function
                      //   mallocKeyInfo() in sqlite3.cpp. We changed all places
                      //   where keyinfos are allocated to call this function.
                      //   We also added a corresponding freeKeyinfo(), and
                      //   changed all locations to free using this function.
  sqlite3 *db;        /* The database connection */
  u8 enc;             /* Text encoding - one of the SQLITE_UTF* values */
  u16 nField;         /* Number of entries in aColl[] */
  u8 *aSortOrder;     /* Sort order for each column.  May be NULL */
  CollSeq *aColl[1];  /* Collating sequence for each term of the key */
};

/*
** An instance of the following structure holds information about a
** single index record that has already been parsed out into individual
** values.
**
** A record is an object that contains one or more fields of data.
** Records are used to store the content of a table row and to store
** the key of an index.  A blob encoding of a record is created by
** the OP_MakeRecord opcode of the VDBE and is disassembled by the
** OP_Column opcode.
**
** This structure holds a record that has already been disassembled
** into its constituent fields.
*/
struct UnpackedRecord {
  KeyInfo *pKeyInfo;  /* Collation and sort-order information */
  u16 nField;         /* Number of entries in apMem[] */
  u16 flags;          /* Boolean settings.  UNPACKED_... below */
  i64 rowid;          /* Used by UNPACKED_PREFIX_SEARCH */
  Mem *aMem;          /* Values */
};

/*
** Allowed values of UnpackedRecord.flags
*/
#define UNPACKED_NEED_FREE     0x0001  /* Memory is from sqlite3Malloc() */
#define UNPACKED_NEED_DESTROY  0x0002  /* apMem[]s should all be destroyed */
#define UNPACKED_IGNORE_ROWID  0x0004  /* Ignore trailing rowid on key1 */
#define UNPACKED_INCRKEY       0x0008  /* Make this key an epsilon larger */
#define UNPACKED_PREFIX_MATCH  0x0010  /* A prefix match is considered OK */
#define UNPACKED_PREFIX_SEARCH 0x0020  /* A prefix match is considered OK */

/*
** Each SQL index is represented in memory by an
** instance of the following structure.
**
** The columns of the table that are to be indexed are described
** by the aiColumn[] field of this structure.  For example, suppose
** we have the following table and index:
**
**     CREATE TABLE Ex1(c1 int, c2 int, c3 text);
**     CREATE INDEX Ex2 ON Ex1(c3,c1);
**
** In the Table structure describing Ex1, nCol==3 because there are
** three columns in the table.  In the Index structure describing
** Ex2, nColumn==2 since 2 of the 3 columns of Ex1 are indexed.
** The value of aiColumn is {2, 0}.  aiColumn[0]==2 because the 
** first column to be indexed (c3) has an index of 2 in Ex1.aCol[].
** The second column to be indexed (c1) has an index of 0 in
** Ex1.aCol[], hence Ex2.aiColumn[1]==0.
**
** The Index.onError field determines whether or not the indexed columns
** must be unique and what to do if they are not.  When Index.onError=OE_None,
** it means this is not a unique index.  Otherwise it is a unique index
** and the value of Index.onError indicate the which conflict resolution 
** algorithm to employ whenever an attempt is made to insert a non-unique
** element.
*/
struct Index {
  char *zName;     /* Name of this index */
  int nColumn;     /* Number of columns in the table used by this index */
  int *aiColumn;   /* Which columns are used by this index.  1st is 0 */
  unsigned *aiRowEst; /* Result of ANALYZE: Est. rows selected by each column */
  Table *pTable;   /* The SQL table being indexed */
  Pgno tnum;       /* Page containing root of this index in database file */
  u8 onError;      /* OE_Abort, OE_Ignore, OE_Replace, or OE_None */
  u8 autoIndex;    /* True if is automatically created (ex: by UNIQUE) */
  u8 bUnordered;   /* Use this index for == or IN queries only */
  char *zColAff;   /* String defining the affinity of each column */
  Index *pNext;    /* The next index associated with the same table */
  Schema *pSchema; /* Schema containing this index */
  u8 *aSortOrder;  /* Array of size Index.nColumn. True==DESC, False==ASC */
  char **azColl;   /* Array of collation sequence names for index */
  IndexSample *aSample;    /* Array of SQLITE_INDEX_SAMPLES samples */
};

/*
** Each sample stored in the sqlite_stat2 table is represented in memory 
** using a structure of this type.
*/
struct IndexSample {
  union {
    char *z;        /* Value if eType is SQLITE_TEXT or SQLITE_BLOB */
    double r;       /* Value if eType is SQLITE_FLOAT or SQLITE_INTEGER */
  } u;
  u8 eType;         /* SQLITE_NULL, SQLITE_INTEGER ... etc. */
  u8 nByte;         /* Size in byte of text or blob. */
};

/*
** Each token coming out of the lexer is an instance of
** this structure.  Tokens are also used as part of an expression.
**
** Note if Token.z==0 then Token.dyn and Token.n are undefined and
** may contain random values.  Do not make any assumptions about Token.dyn
** and Token.n when Token.z==0.
*/
struct Token {
  const char *z;     /* Text of the token.  Not NULL-terminated! */
  unsigned int n;    /* Number of characters in this token */
};

/*
** An instance of this structure contains information needed to generate
** code for a SELECT that contains aggregate functions.
**
** If Expr.op==TK_AGG_COLUMN or TK_AGG_FUNCTION then Expr.pAggInfo is a
** pointer to this structure.  The Expr.iColumn field is the index in
** AggInfo.aCol[] or AggInfo.aFunc[] of information needed to generate
** code for that node.
**
** AggInfo.pGroupBy and AggInfo.aFunc.pExpr point to fields within the
** original Select structure that describes the SELECT statement.  These
** fields do not need to be freed when deallocating the AggInfo structure.
*/
struct AggInfo_col {    /* For each column used in source tables */
  Table *pTab;             /* Source table */
  int iTable;              /* Cursor number of the source table */
  int iColumn;             /* Column number within the source table */
  int iSorterColumn;       /* Column number in the sorting index */
  int iMem;                /* Memory location that acts as accumulator */
  Expr *pExpr;             /* The original expression */
};
struct AggInfo_func {   /* For each aggregate function */
  Expr *pExpr;             /* Expression encoding the function */
  FuncDef *pFunc;          /* The aggregate function implementation */
  int iMem;                /* Memory location that acts as accumulator */
  int iDistinct;           /* Ephemeral table used to enforce DISTINCT */
};

struct AggInfo {
  u8 directMode;          /* Direct rendering mode means take data directly
                          ** from source tables rather than from accumulators */
  u8 useSortingIdx;       /* In direct mode, reference the sorting index rather
                          ** than the source table */
  int sortingIdx;         /* Cursor number of the sorting index */
  ExprList *pGroupBy;     /* The group by clause */
  int nSortingColumn;     /* Number of columns in the sorting index */
  struct AggInfo_col *aCol;
  int nColumn;            /* Number of used entries in aCol[] */
  int nColumnAlloc;       /* Number of slots allocated for aCol[] */
  int nAccumulator;       /* Number of columns that show through to the output.
                          ** Additional columns are used only as parameters to
                          ** aggregate functions */
  struct AggInfo_func *aFunc;
  int nFunc;              /* Number of entries in aFunc[] */
  int nFuncAlloc;         /* Number of slots allocated for aFunc[] */
};

/*
** The datatype ynVar is a signed integer, either 16-bit or 32-bit.
** Usually it is 16-bits.  But if SQLITE_MAX_VARIABLE_NUMBER is greater
** than 32767 we have to make it 32-bit.  16-bit is preferred because
** it uses less memory in the Expr object, which is a big memory user
** in systems with lots of prepared statements.  And few applications
** need more than about 10 or 20 variables.  But some extreme users want
** to have prepared statements with over 32767 variables, and for them
** the option is available (at compile-time).
*/
#if SQLITE_MAX_VARIABLE_NUMBER<=32767
typedef i16 ynVar;
#else
typedef int ynVar;
#endif

/*
** Each node of an expression in the parse tree is an instance
** of this structure.
**
** Expr.op is the opcode. The integer parser token codes are reused
** as opcodes here. For example, the parser defines TK_GE to be an integer
** code representing the ">=" operator. This same integer code is reused
** to represent the greater-than-or-equal-to operator in the expression
** tree.
**
** If the expression is an SQL literal (TK_INTEGER, TK_FLOAT, TK_BLOB, 
** or TK_STRING), then Expr.token contains the text of the SQL literal. If
** the expression is a variable (TK_VARIABLE), then Expr.token contains the 
** variable name. Finally, if the expression is an SQL function (TK_FUNCTION),
** then Expr.token contains the name of the function.
**
** Expr.pRight and Expr.pLeft are the left and right subexpressions of a
** binary operator. Either or both may be NULL.
**
** Expr.x.pList is a list of arguments if the expression is an SQL function,
** a CASE expression or an IN expression of the form "<lhs> IN (<y>, <z>...)".
** Expr.x.pSelect is used if the expression is a sub-select or an expression of
** the form "<lhs> IN (SELECT ...)". If the EP_xIsSelect bit is set in the
** Expr.flags mask, then Expr.x.pSelect is valid. Otherwise, Expr.x.pList is 
** valid.
**
** An expression of the form ID or ID.ID refers to a column in a table.
** For such expressions, Expr.op is set to TK_COLUMN and Expr.iTable is
** the integer cursor number of a VDBE cursor pointing to that table and
** Expr.iColumn is the column number for the specific column.  If the
** expression is used as a result in an aggregate SELECT, then the
** value is also stored in the Expr.iAgg column in the aggregate so that
** it can be accessed after all aggregates are computed.
**
** If the expression is an unbound variable marker (a question mark 
** character '?' in the original SQL) then the Expr.iTable holds the index 
** number for that variable.
**
** If the expression is a subquery then Expr.iColumn holds an integer
** register number containing the result of the subquery.  If the
** subquery gives a constant result, then iTable is -1.  If the subquery
** gives a different answer at different times during statement processing
** then iTable is the address of a subroutine that computes the subquery.
**
** If the Expr is of type OP_Column, and the table it is selecting from
** is a disk table or the "old.*" pseudo-table, then pTab points to the
** corresponding table definition.
**
** ALLOCATION NOTES:
**
** Expr objects can use a lot of memory space in database schema.  To
** help reduce memory requirements, sometimes an Expr object will be
** truncated.  And to reduce the number of memory allocations, sometimes
** two or more Expr objects will be stored in a single memory allocation,
** together with Expr.zToken strings.
**
** If the EP_Reduced and EP_TokenOnly flags are set when
** an Expr object is truncated.  When EP_Reduced is set, then all
** the child Expr objects in the Expr.pLeft and Expr.pRight subtrees
** are contained within the same memory allocation.  Note, however, that
** the subtrees in Expr.x.pList or Expr.x.pSelect are always separately
** allocated, regardless of whether or not EP_Reduced is set.
*/
struct Expr {
  u8 op;                 /* Operation performed by this node */
  char affinity;         /* The affinity of the column or 0 if not a column */
  u16 flags;             /* Various flags.  EP_* See below */
  union {
    char *zToken;          /* Token value. Zero terminated and dequoted */
    int iValue;            /* Non-negative integer value if EP_IntValue */
  } u;

  /* If the EP_TokenOnly flag is set in the Expr.flags mask, then no
  ** space is allocated for the fields below this point. An attempt to
  ** access them will result in a segfault or malfunction. 
  *********************************************************************/

  Expr *pLeft;           /* Left subnode */
  Expr *pRight;          /* Right subnode */
  union {
    ExprList *pList;     /* Function arguments or in "<expr> IN (<expr-list)" */
    Select *pSelect;     /* Used for sub-selects and "<expr> IN (<select>)" */
  } x;
  CollSeq *pColl;        /* The collation type of the column or 0 */

  /* If the EP_Reduced flag is set in the Expr.flags mask, then no
  ** space is allocated for the fields below this point. An attempt to
  ** access them will result in a segfault or malfunction.
  *********************************************************************/

  int iTable;            /* TK_COLUMN: cursor number of table holding column
                         ** TK_REGISTER: register number
                         ** TK_TRIGGER: 1 -> new, 0 -> old */
  ynVar iColumn;         /* TK_COLUMN: column index.  -1 for rowid.
                         ** TK_VARIABLE: variable number (always >= 1). */
  i16 iAgg;              /* Which entry in pAggInfo->aCol[] or ->aFunc[] */
  i16 iRightJoinTable;   /* If EP_FromJoin, the right table of the join */
  u8 flags2;             /* Second set of flags.  EP2_... */
  u8 op2;                /* If a TK_REGISTER, the original value of Expr.op */
  AggInfo *pAggInfo;     /* Used by TK_AGG_COLUMN and TK_AGG_FUNCTION */
  Table *pTab;           /* Table for TK_COLUMN expressions. */
#if SQLITE_MAX_EXPR_DEPTH>0
  int nHeight;           /* Height of the tree headed by this node */
#endif
};

/*
** The following are the meanings of bits in the Expr.flags field.
*/
#define EP_FromJoin   0x0001  /* Originated in ON or USING clause of a join */
#define EP_Agg        0x0002  /* Contains one or more aggregate functions */
#define EP_Resolved   0x0004  /* IDs have been resolved to COLUMNs */
#define EP_Error      0x0008  /* Expression contains one or more errors */
#define EP_Distinct   0x0010  /* Aggregate function with DISTINCT keyword */
#define EP_VarSelect  0x0020  /* pSelect is correlated, not constant */
#define EP_DblQuoted  0x0040  /* token.z was originally in "..." */
#define EP_InfixFunc  0x0080  /* True for an infix function: LIKE, GLOB, etc */
#define EP_ExpCollate 0x0100  /* Collating sequence specified explicitly */
#define EP_FixedDest  0x0200  /* Result needed in a specific register */
#define EP_IntValue   0x0400  /* Integer value contained in u.iValue */
#define EP_xIsSelect  0x0800  /* x.pSelect is valid (otherwise x.pList is) */

#define EP_Reduced    0x1000  /* Expr struct is EXPR_REDUCEDSIZE bytes only */
#define EP_TokenOnly  0x2000  /* Expr struct is EXPR_TOKENONLYSIZE bytes only */
#define EP_Static     0x4000  /* Held in memory not obtained from malloc() */

/*
** The following are the meanings of bits in the Expr.flags2 field.
*/
#define EP2_MallocedToken  0x0001  /* Need to sqlite3DbFree() Expr.zToken */
#define EP2_Irreducible    0x0002  /* Cannot EXPRDUP_REDUCE this Expr */

/*
** The pseudo-routine sqlite3ExprSetIrreducible sets the EP2_Irreducible
** flag on an expression structure.  This flag is used for VV&A only.  The
** routine is implemented as a macro that only works when in debugging mode,
** so as not to burden production code.
*/
#ifdef SQLITE_DEBUG
# define ExprSetIrreducible(X)  (X)->flags2 |= EP2_Irreducible
#else
# define ExprSetIrreducible(X)
#endif

/*
** These macros can be used to test, set, or clear bits in the 
** Expr.flags field.
*/
#define ExprHasProperty(E,P)     (((E)->flags&(P))==(P))
#define ExprHasAnyProperty(E,P)  (((E)->flags&(P))!=0)
#define ExprSetProperty(E,P)     (E)->flags|=(P)
#define ExprClearProperty(E,P)   (E)->flags&=~(P)

/*
** Macros to determine the number of bytes required by a normal Expr 
** struct, an Expr struct with the EP_Reduced flag set in Expr.flags 
** and an Expr struct with the EP_TokenOnly flag set.
*/
#define EXPR_FULLSIZE           sizeof(Expr)           /* Full size */
#define EXPR_REDUCEDSIZE        OFFSETOF(Expr,iTable)  /* Common features */
#define EXPR_TOKENONLYSIZE      OFFSETOF(Expr,pLeft)   /* Fewer features */

/*
** Flags passed to the sqlite3ExprDup() function. See the header comment 
** above sqlite3ExprDup() for details.
*/
#define EXPRDUP_REDUCE         0x0001  /* Used reduced-size Expr nodes */

/*
** A list of expressions.  Each expression may optionally have a
** name.  An expr/name combination can be used in several ways, such
** as the list of "expr AS ID" fields following a "SELECT" or in the
** list of "ID = expr" items in an UPDATE.  A list of expressions can
** also be used as the argument to a function, in which case the a.zName
** field is not used.
*/
  struct ExprList_item {
    Expr *pExpr;           /* The list of expressions */
    char *zName;           /* Token associated with this expression */
    char *zSpan;           /* Original text of the expression */
    u8 sortOrder;          /* 1 for DESC or 0 for ASC */
    u8 done;               /* A flag to indicate when processing is finished */
    u16 iCol;              /* For ORDER BY, column number in result set */
    u16 iAlias;            /* Index into Parse.aAlias[] for zName */
  };
struct ExprList {
  int nExpr;             /* Number of expressions on the list */
  int nAlloc;            /* Number of entries allocated below */
  int iECursor;          /* VDBE Cursor associated with this ExprList */
  struct ExprList_item *a;                  /* One entry for each expression */
};

/*
** An instance of this structure is used by the parser to record both
** the parse tree for an expression and the span of input text for an
** expression.
*/
struct ExprSpan {
  Expr *pExpr;          /* The expression parse tree */
  const char *zStart;   /* First character of input text */
  const char *zEnd;     /* One character past the end of input text */
};

/*
** An instance of this structure can hold a simple list of identifiers,
** such as the list "a,b,c" in the following statements:
**
**      INSERT INTO t(a,b,c) VALUES ...;
**      CREATE INDEX idx ON t(a,b,c);
**      CREATE TRIGGER trig BEFORE UPDATE ON t(a,b,c) ...;
**
** The IdList.a.idx field is used when the IdList represents the list of
** column names after a table name in an INSERT statement.  In the statement
**
**     INSERT INTO t(a,b,c) ...
**
** If "a" is the k-th column of table "t", then IdList.a[0].idx==k.
*/
  struct IdList_item {
    char *zName;      /* Name of the identifier */
    int idx;          /* Index in some Table.aCol[] of a column named zName */
  };

struct IdList {
  struct IdList_item *a;
  int nId;         /* Number of identifiers on the list */
  int nAlloc;      /* Number of entries allocated for a[] below */
};

/*
** The bitmask datatype defined below is used for various optimizations.
**
** Changing this from a 64-bit to a 32-bit type limits the number of
** tables in a join to 32 instead of 64.  But it also reduces the size
** of the library by 738 bytes on ix86.
*/
typedef u64 Bitmask;

/*
** The number of bits in a Bitmask.  "BMS" means "BitMask Size".
*/
#define BMS  ((int)(sizeof(Bitmask)*8))

/*
** The following structure describes the FROM clause of a SELECT statement.
** Each table or subquery in the FROM clause is a separate element of
** the SrcList.a[] array.
**
** With the addition of multiple database support, the following structure
** can also be used to describe a particular table such as the table that
** is modified by an INSERT, DELETE, or UPDATE statement.  In standard SQL,
** such a table must be a simple name: ID.  But in SQLite, the table can
** now be identified by a database name, a dot, then the table name: ID.ID.
**
** The jointype starts out showing the join type between the current table
** and the next table on the list.  The parser builds the list this way.
** But sqlite3SrcListShiftJoinType() later shifts the jointypes so that each
** jointype expresses the join between the table and the previous table.
**
** In the colUsed field, the high-order bit (bit 63) is set if the table
** contains more than 63 columns and the 64-th or later column is used.
*/
struct SrcList_item {
    char *zDatabase;  /* Name of database holding this table */
    char *zName;      /* Name of the table */
    char *zAlias;     /* The "B" part of a "A AS B" phrase.  zName is the "A" */
    Table *pTab;      /* An SQL table corresponding to zName */
    Select *pSelect;  /* A SELECT statement used in place of a table name */
    u8 isPopulated;   /* Temporary table associated with SELECT is populated */
    u8 jointype;      /* Type of join between this able and the previous */
    u8 notIndexed;    /* True if there is a NOT INDEXED clause */
#ifndef SQLITE_OMIT_EXPLAIN
    u8 iSelectId;     /* If pSelect!=0, the id of the sub-select in EQP */
#endif
    int iCursor;      /* The VDBE cursor number used to access this table */
    Expr *pOn;        /* The ON clause of a join */
    IdList *pUsing;   /* The USING clause of a join */
    Bitmask colUsed;  /* Bit N (1<<N) set if column N of pTab is used */
    char *zIndex;     /* Identifier from "INDEXED BY <zIndex>" clause */
    Index *pIndex;    /* Index structure corresponding to zIndex, if any */
  };

struct SrcList {
  i16 nSrc;        /* Number of tables or subqueries in the FROM clause */
  i16 nAlloc;      /* Number of entries allocated in a[] below */
  struct SrcList_item a[1];             /* One entry for each identifier on the list */
};

/*
** Permitted values of the SrcList.a.jointype field
*/
#define JT_INNER     0x0001    /* Any kind of inner or cross join */
#define JT_CROSS     0x0002    /* Explicit use of the CROSS keyword */
#define JT_NATURAL   0x0004    /* True for a "natural" join */
#define JT_LEFT      0x0008    /* Left outer join */
#define JT_RIGHT     0x0010    /* Right outer join */
#define JT_OUTER     0x0020    /* The "OUTER" keyword is present */
#define JT_ERROR     0x0040    /* unknown or unsupported join type */


/*
** A WherePlan object holds information that describes a lookup
** strategy.
**
** This object is intended to be opaque outside of the where.c module.
** It is included here only so that that compiler will know how big it
** is.  None of the fields in this object should be used outside of
** the where.c module.
**
** Within the union, pIdx is only used when wsFlags&WHERE_INDEXED is true.
** pTerm is only used when wsFlags&WHERE_MULTI_OR is true.  And pVtabIdx
** is only used when wsFlags&WHERE_VIRTUALTABLE is true.  It is never the
** case that more than one of these conditions is true.
*/
struct WherePlan {
  u32 wsFlags;                   /* WHERE_* flags that describe the strategy */
  u32 nEq;                       /* Number of == constraints */
  double nRow;                   /* Estimated number of rows (for EQP) */
  union {
    Index *pIdx;                   /* Index when WHERE_INDEXED is true */
    struct WhereTerm *pTerm;       /* WHERE clause term for OR-search */
    sqlite3_index_info *pVtabIdx;  /* Virtual table index to use */
  } u;
};

/*
** For each nested loop in a WHERE clause implementation, the WhereInfo
** structure contains a single instance of this structure.  This structure
** is intended to be private the the where.c module and should not be
** access or modified by other modules.
**
** The pIdxInfo field is used to help pick the best index on a
** virtual table.  The pIdxInfo pointer contains indexing
** information for the i-th table in the FROM clause before reordering.
** All the pIdxInfo pointers are freed by whereInfoFree() in where.c.
** All other information in the i-th WhereLevel object for the i-th table
** after FROM clause ordering.
*/
struct InLoop {
        int iCur;              /* The VDBE cursor used by this IN operator */
        int addrInTop;         /* Top of the IN loop */
};

struct WhereLevel {
  WherePlan plan;       /* query plan for this element of the FROM clause */
  int iLeftJoin;        /* Memory cell used to implement LEFT OUTER JOIN */
  int iTabCur;          /* The VDBE cursor used to access the table */
  int iIdxCur;          /* The VDBE cursor used to access pIdx */
  int addrBrk;          /* Jump here to break out of the loop */
  int addrNxt;          /* Jump here to start the next IN combination */
  int addrCont;         /* Jump here to continue with the next loop cycle */
  int addrFirst;        /* First instruction of interior of the loop */
  u8 iFrom;             /* Which entry in the FROM clause */
  u8 op, p5;            /* Opcode and P5 of the opcode that ends the loop */
  i64 p1, p2;           /* Operands of the opcode used to ends the loop */
  union {               /* Information that depends on plan.wsFlags */
    struct {
      int nIn;              /* Number of entries in aInLoop[] */
      struct InLoop *aInLoop;           /* Information about each nested IN operator */
    } in;                 /* Used when plan.wsFlags&WHERE_IN_ABLE */
  } u;

  /* The following field is really not part of the current level.  But
  ** we need a place to cache virtual table index information for each
  ** virtual table in the FROM clause and the WhereLevel structure is
  ** a convenient place since there is one WhereLevel for each FROM clause
  ** element.
  */
  sqlite3_index_info *pIdxInfo;  /* Index info for n-th source table */
};

/*
** Flags appropriate for the wctrlFlags parameter of sqlite3WhereBegin()
** and the WhereInfo.wctrlFlags member.
*/
#define WHERE_ORDERBY_NORMAL   0x0000 /* No-op */
#define WHERE_ORDERBY_MIN      0x0001 /* ORDER BY processing for min() func */
#define WHERE_ORDERBY_MAX      0x0002 /* ORDER BY processing for max() func */
#define WHERE_ONEPASS_DESIRED  0x0004 /* Want to do one-pass UPDATE/DELETE */
#define WHERE_DUPLICATES_OK    0x0008 /* Ok to return a row more than once */
#define WHERE_OMIT_OPEN        0x0010 /* Table cursors are already open */
#define WHERE_OMIT_CLOSE       0x0020 /* Omit close of table & index cursors */
#define WHERE_FORCE_TABLE      0x0040 /* Do not use an index-only search */
#define WHERE_ONETABLE_ONLY    0x0080 /* Only code the 1st table in pTabList */

/*
** The WHERE clause processing routine has two halves.  The
** first part does the start of the WHERE loop and the second
** half does the tail of the WHERE loop.  An instance of
** this structure is returned by the first half and passed
** into the second half to give some continuity.
*/
struct WhereInfo {
  Parse *pParse;       /* Parsing and code generating context */
  u16 wctrlFlags;      /* Flags originally passed to sqlite3WhereBegin() */
  u8 okOnePass;        /* Ok to use one-pass algorithm for UPDATE or DELETE */
  u8 untestedTerms;    /* Not all WHERE terms resolved by outer loop */
  SrcList *pTabList;             /* List of tables in the join */
  int iTop;                      /* The very beginning of the WHERE loop */
  int iContinue;                 /* Jump here to continue with next record */
  int iBreak;                    /* Jump here to break out of the loop */
  int nLevel;                    /* Number of nested loop */
  struct WhereClause *pWC;       /* Decomposition of the WHERE clause */
  double savedNQueryLoop;        /* pParse->nQueryLoop outside the WHERE loop */
  double nRowOut;                /* Estimated number of output rows */
  WhereLevel a[1];               /* Information about each nest loop in WHERE */
};

/*
** A NameContext defines a context in which to resolve table and column
** names.  The context consists of a list of tables (the pSrcList) field and
** a list of named expression (pEList).  The named expression list may
** be NULL.  The pSrc corresponds to the FROM clause of a SELECT or
** to the table being operated on by INSERT, UPDATE, or DELETE.  The
** pEList corresponds to the result set of a SELECT and is NULL for
** other statements.
**
** NameContexts can be nested.  When resolving names, the inner-most 
** context is searched first.  If no match is found, the next outer
** context is checked.  If there is still no match, the next context
** is checked.  This process continues until either a match is found
** or all contexts are check.  When a match is found, the nRef member of
** the context containing the match is incremented. 
**
** Each subquery gets a new NameContext.  The pNext field points to the
** NameContext in the parent query.  Thus the process of scanning the
** NameContext list corresponds to searching through successively outer
** subqueries looking for a match.
*/
struct NameContext {
  Parse *pParse;       /* The parser */
  SrcList *pSrcList;   /* One or more tables used to resolve names */
  ExprList *pEList;    /* Optional list of named expressions */
  int nRef;            /* Number of names resolved by this context */
  int nErr;            /* Number of errors encountered while resolving names */
  u8 allowAgg;         /* Aggregate functions allowed here */
  u8 hasAgg;           /* True if aggregates are seen */
  u8 isCheck;          /* True if resolving names in a CHECK constraint */
  int nDepth;          /* Depth of subquery recursion. 1 for no recursion */
  AggInfo *pAggInfo;   /* Information about aggregates at this level */
  NameContext *pNext;  /* Next outer name context.  NULL for outermost */
};

/*
** An instance of the following structure contains all information
** needed to generate code for a single SELECT statement.
**
** nLimit is set to -1 if there is no LIMIT clause.  nOffset is set to 0.
** If there is a LIMIT clause, the parser sets nLimit to the value of the
** limit and nOffset to the value of the offset (or 0 if there is not
** offset).  But later on, nLimit and nOffset become the memory locations
** in the VDBE that record the limit and offset counters.
**
** addrOpenEphm[] entries contain the address of OP_OpenEphemeral opcodes.
** These addresses must be stored so that we can go back and fill in
** the P4_KEYINFO and P2 parameters later.  Neither the KeyInfo nor
** the number of columns in P2 can be computed at the same time
** as the OP_OpenEphm instruction is coded because not
** enough information about the compound query is known at that point.
** The KeyInfo for addrOpenTran[0] and [1] contains collating sequences
** for the result set.  The KeyInfo for addrOpenTran[2] contains collating
** sequences for the ORDER BY clause.
*/
struct Select {
  ExprList *pEList;      /* The fields of the result */
  u8 op;                 /* One of: TK_UNION TK_ALL TK_INTERSECT TK_EXCEPT */
  char affinity;         /* MakeRecord with this affinity for SRT_Set */
  u16 selFlags;          /* Various SF_* values */
  SrcList *pSrc;         /* The FROM clause */
  Expr *pWhere;          /* The WHERE clause */
  ExprList *pGroupBy;    /* The GROUP BY clause */
  Expr *pHaving;         /* The HAVING clause */
  ExprList *pOrderBy;    /* The ORDER BY clause */
  Select *pPrior;        /* Prior select in a compound select statement */
  Select *pNext;         /* Next select to the left in a compound */
  Select *pRightmost;    /* Right-most select in a compound select statement */
  Expr *pLimit;          /* LIMIT expression. NULL means not used. */
  Expr *pOffset;         /* OFFSET expression. NULL means not used. */
  int iLimit, iOffset;   /* Memory registers holding LIMIT & OFFSET counters */
  int addrOpenEphm[3];   /* OP_OpenEphem opcodes related to this select */
  double nSelectRow;     /* Estimated number of result rows */
};

/*
** Allowed values for Select.selFlags.  The "SF" prefix stands for
** "Select Flag".
*/
#define SF_Distinct        0x0001  /* Output should be DISTINCT */
#define SF_Resolved        0x0002  /* Identifiers have been resolved */
#define SF_Aggregate       0x0004  /* Contains aggregate functions */
#define SF_UsesEphemeral   0x0008  /* Uses the OpenEphemeral opcode */
#define SF_Expanded        0x0010  /* sqlite3SelectExpand() called on this */
#define SF_HasTypeInfo     0x0020  /* FROM subqueries have Table metadata */


/*
** The results of a select can be distributed in several ways.  The
** "SRT" prefix means "SELECT Result Type".
*/
#define SRT_Union        1  /* Store result as keys in an index */
#define SRT_Except       2  /* Remove result from a UNION index */
#define SRT_Exists       3  /* Store 1 if the result is not empty */
#define SRT_Discard      4  /* Do not save the results anywhere */

/* The ORDER BY clause is ignored for all of the above */
#define IgnorableOrderby(X) ((X->eDest)<=SRT_Discard)

#define SRT_Output       5  /* Output each row of result */
#define SRT_Mem          6  /* Store result in a memory cell */
#define SRT_Set          7  /* Store results as keys in an index */
#define SRT_Table        8  /* Store result as data with an automatic rowid */
#define SRT_EphemTab     9  /* Create transient tab and store like SRT_Table */
#define SRT_Coroutine   10  /* Generate a single row of result */

/*
** A structure used to customize the behavior of sqlite3Select(). See
** comments above sqlite3Select() for details.
*/
typedef struct SelectDest SelectDest;
struct SelectDest {
  u8 eDest;         /* How to dispose of the results */
  u8 affinity;      /* Affinity used when eDest==SRT_Set */
  int iParm;        /* A parameter used by the eDest disposal method */
  int iMem;         /* Base register where results are written */
  int nMem;         /* Number of registers allocated */
};

/*
** During code generation of statements that do inserts into AUTOINCREMENT 
** tables, the following information is attached to the Table.u.autoInc.p
** pointer of each autoincrement table to record some side information that
** the code generator needs.  We have to keep per-table autoincrement
** information in case inserts are down within triggers.  Triggers do not
** normally coordinate their activities, but we do need to coordinate the
** loading and saving of autoincrement information.
*/
struct AutoincInfo {
  AutoincInfo *pNext;   /* Next info block in a list of them all */
  Table *pTab;          /* Table this info block refers to */
  int iDb;              /* Index in sqlite3.aDb[] of database holding pTab */
  int regCtr;           /* Memory register holding the rowid counter */
};

/*
** Size of the column cache
*/
#ifndef SQLITE_N_COLCACHE
# define SQLITE_N_COLCACHE 10
#endif

/*
** At least one instance of the following structure is created for each 
** trigger that may be fired while parsing an INSERT, UPDATE or DELETE
** statement. All such objects are stored in the linked list headed at
** Parse.pTriggerPrg and deleted once statement compilation has been
** completed.
**
** A Vdbe sub-program that implements the body and WHEN clause of trigger
** TriggerPrg.pTrigger, assuming a default ON CONFLICT clause of
** TriggerPrg.orconf, is stored in the TriggerPrg.pProgram variable.
** The Parse.pTriggerPrg list never contains two entries with the same
** values for both pTrigger and orconf.
**
** The TriggerPrg.aColmask[0] variable is set to a mask of old.* columns
** accessed (or set to 0 for triggers fired as a result of INSERT 
** statements). Similarly, the TriggerPrg.aColmask[1] variable is set to
** a mask of new.* columns used by the program.
*/
struct TriggerPrg {
  Trigger *pTrigger;      /* Trigger this program was coded from */
  int orconf;             /* Default ON CONFLICT policy */
  SubProgram *pProgram;   /* Program implementing pTrigger/orconf */
  u32 aColmask[2];        /* Masks of old.*, new.* columns accessed */
  TriggerPrg *pNext;      /* Next entry in Parse.pTriggerPrg list */
};

/*
** The yDbMask datatype for the bitmask of all attached databases.
*/
#if SQLITE_MAX_ATTACHED>30
  typedef sqlite3_uint64 yDbMask;
#else
  typedef unsigned int yDbMask;
#endif

/*
** An SQL parser context.  A copy of this structure is passed through
** the parser and down into all the parser action routine in order to
** carry around information that is global to the entire parse.
**
** The structure is divided into two parts.  When the parser and code
** generate call themselves recursively, the first part of the structure
** is constant but the second part is reset at the beginning and end of
** each recursion.
**
** The nTableLock and aTableLock variables are only used if the shared-cache 
** feature is enabled (if sqlite3Tsd()->useSharedData is true). They are
** used to store the set of table-locks required by the statement being
** compiled. Function sqlite3TableLock() is used to add entries to the
** list.
*/
  struct yColCache {
    int iTable;           /* Table cursor number */
    int iColumn;          /* Table column number */
    u8 tempReg;           /* iReg is a temp register that needs to be freed */
    int iLevel;           /* Nesting level */
    int iReg;             /* Reg with value of this column. 0 means none. */
    int lru;              /* Least recently used entry has the smallest value */
  };

struct Parse {
  sqlite3 *db;         /* The main database structure */
  int rc;              /* Return code from execution */
  char *zErrMsg;       /* An error message */
  Vdbe *pVdbe;         /* An engine for executing database bytecode */
  u8 colNamesSet;      /* TRUE after OP_ColumnName has been issued to pVdbe */
  u8 nameClash;        /* A permanent table name clashes with temp table name */
  u8 checkSchema;      /* Causes schema cookie check after an error */
  u8 nested;           /* Number of nested calls to the parser/code generator */
  u8 parseError;       /* True after a parsing error.  Ticket #1794 */
  u8 nTempReg;         /* Number of temporary registers in aTempReg[] */
  u8 nTempInUse;       /* Number of aTempReg[] currently checked out */
  int aTempReg[8];     /* Holding area for temporary registers */
  int nRangeReg;       /* Size of the temporary register block */
  int iRangeReg;       /* First register in temporary register block */
  int nErr;            /* Number of errors seen */
  int nTab;            /* Number of previously allocated VDBE cursors */
  int nMem;            /* Number of memory cells used so far */
  int nSet;            /* Number of sets used so far */
  int ckBase;          /* Base register of data during check constraints */
  int iCacheLevel;     /* ColCache valid when aColCache[].iLevel<=iCacheLevel */
  int iCacheCnt;       /* Counter used to generate aColCache[].lru values */
  u8 nColCache;        /* Number of entries in the column cache */
  u8 iColCache;        /* Next entry of the cache to replace */
  struct yColCache aColCache[SQLITE_N_COLCACHE];  /* One for each column cache entry */
  yDbMask writeMask;   /* Start a write transaction on these databases */
  yDbMask cookieMask;  /* Bitmask of schema verified databases */
  u8 isMultiWrite;     /* True if statement may affect/insert multiple rows */
  u8 mayAbort;         /* True if statement may throw an ABORT exception */
  int cookieGoto;      /* Address of OP_Goto to cookie verifier subroutine */
  int cookieValue[SQLITE_MAX_ATTACHED+2];  /* Values of cookies to verify */
#ifndef SQLITE_OMIT_SHARED_CACHE
  int nTableLock;        /* Number of locks in aTableLock */
  TableLock *aTableLock; /* Required table locks for shared-cache mode */
#endif
  u64 regRowid;        /* Register holding rowid of CREATE TABLE entry */
  int regRoot;         /* Register holding root page number for new objects */
  AutoincInfo *pAinc;  /* Information about AUTOINCREMENT counters */
  int nMaxArg;         /* Max args passed to user function by sub-program */

  /* Information used while coding trigger programs. */
  Parse *pToplevel;    /* Parse structure for main program (or NULL) */
  Table *pTriggerTab;  /* Table triggers are being coded for */
  u32 oldmask;         /* Mask of old.* columns referenced */
  u32 newmask;         /* Mask of new.* columns referenced */
  u8 eTriggerOp;       /* TK_UPDATE, TK_INSERT or TK_DELETE */
  u8 eOrconf;          /* Default ON CONFLICT policy for trigger steps */
  u8 disableTriggers;  /* True to disable triggers */
  double nQueryLoop;   /* Estimated number of iterations of a query */

  /* Above is constant between recursions.  Below is reset before and after
  ** each recursion */

  int nVar;            /* Number of '?' variables seen in the SQL so far */
  int nVarExpr;        /* Number of used slots in apVarExpr[] */
  int nVarExprAlloc;   /* Number of allocated slots in apVarExpr[] */
  Expr **apVarExpr;    /* Pointers to :aaa and $aaaa wildcard expressions */
  Vdbe *pReprepare;    /* VM being reprepared (sqlite3Reprepare()) */
  int nAlias;          /* Number of aliased result set columns */
  int nAliasAlloc;     /* Number of allocated slots for aAlias[] */
  int *aAlias;         /* Register used to hold aliased result */
  u8 explain;          /* True if the EXPLAIN flag is found on the query */
  Token sNameToken;    /* Token with unqualified schema object name */
  Token sLastToken;    /* The last token parsed */
  const char *zTail;   /* All SQL text past the last semicolon parsed */
  Table *pNewTable;    /* A table being constructed by CREATE TABLE */
  Trigger *pNewTrigger;     /* Trigger under construct by a CREATE TRIGGER */
  const char *zAuthContext; /* The 6th parameter to db->xAuth callbacks */
#ifndef SQLITE_OMIT_VIRTUALTABLE
  Token sArg;                /* Complete text of a module argument */
  u8 declareVtab;            /* True if inside sqlite3_declare_vtab() */
  int nVtabLock;             /* Number of virtual tables to lock */
  Table **apVtabLock;        /* Pointer to virtual tables needing locking */
#endif
  int nHeight;            /* Expression tree height of current sub-select */
  Table *pZombieTab;      /* List of Table objects to delete after code gen */
  TriggerPrg *pTriggerPrg;    /* Linked list of coded triggers */

#ifndef SQLITE_OMIT_EXPLAIN
  int iSelectId;
  int iNextSelectId;
#endif
};

#ifdef SQLITE_OMIT_VIRTUALTABLE
  #define IN_DECLARE_VTAB 0
#else
  #define IN_DECLARE_VTAB (pParse->declareVtab)
#endif

/*
** An instance of the following structure can be declared on a stack and used
** to save the Parse.zAuthContext value so that it can be restored later.
*/
struct AuthContext {
  const char *zAuthContext;   /* Put saved Parse.zAuthContext here */
  Parse *pParse;              /* The Parse structure */
};

/*
** Bitfield flags for P5 value in OP_Insert and OP_Delete
*/
#define OPFLAG_NCHANGE       0x01    /* Set to update db->nChange */
#define OPFLAG_LASTROWID     0x02    /* Set to update db->lastRowid */
#define OPFLAG_ISUPDATE      0x04    /* This OP_Insert is an sql UPDATE */
#define OPFLAG_APPEND        0x08    /* This is likely to be an append */
#define OPFLAG_USESEEKRESULT 0x10    /* Try to avoid a seek in BtreeInsert() */
#define OPFLAG_CLEARCACHE    0x20    /* Clear pseudo-table cache in OP_Column */

/*
 * Each trigger present in the database schema is stored as an instance of
 * struct Trigger. 
 *
 * Pointers to instances of struct Trigger are stored in two ways.
 * 1. In the "trigHash" hash table (part of the sqlite3* that represents the 
 *    database). This allows Trigger structures to be retrieved by name.
 * 2. All triggers associated with a single table form a linked list, using the
 *    pNext member of struct Trigger. A pointer to the first element of the
 *    linked list is stored as the "pTrigger" member of the associated
 *    struct Table.
 *
 * The "step_list" member points to the first element of a linked list
 * containing the SQL statements specified as the trigger program.
 */
struct Trigger {
  char *zName;            /* The name of the trigger                        */
  char *table;            /* The table or view to which the trigger applies */
  u8 op;                  /* One of TK_DELETE, TK_UPDATE, TK_INSERT         */
  u8 tr_tm;               /* One of TRIGGER_BEFORE, TRIGGER_AFTER */
  Expr *pWhen;            /* The WHEN clause of the expression (may be NULL) */
  IdList *pColumns;       /* If this is an UPDATE OF <column-list> trigger,
                             the <column-list> is stored here */
  Schema *pSchema;        /* Schema containing the trigger */
  Schema *pTabSchema;     /* Schema containing the table */
  TriggerStep *step_list; /* Link list of trigger program steps             */
  Trigger *pNext;         /* Next trigger associated with the table */
};

/*
** A trigger is either a BEFORE or an AFTER trigger.  The following constants
** determine which. 
**
** If there are multiple triggers, you might of some BEFORE and some AFTER.
** In that cases, the constants below can be ORed together.
*/
#define TRIGGER_BEFORE  1
#define TRIGGER_AFTER   2

/*
 * An instance of struct TriggerStep is used to store a single SQL statement
 * that is a part of a trigger-program. 
 *
 * Instances of struct TriggerStep are stored in a singly linked list (linked
 * using the "pNext" member) referenced by the "step_list" member of the 
 * associated struct Trigger instance. The first element of the linked list is
 * the first step of the trigger-program.
 * 
 * The "op" member indicates whether this is a "DELETE", "INSERT", "UPDATE" or
 * "SELECT" statement. The meanings of the other members is determined by the 
 * value of "op" as follows:
 *
 * (op == TK_INSERT)
 * orconf    -> stores the ON CONFLICT algorithm
 * pSelect   -> If this is an INSERT INTO ... SELECT ... statement, then
 *              this stores a pointer to the SELECT statement. Otherwise NULL.
 * target    -> A token holding the quoted name of the table to insert into.
 * pExprList -> If this is an INSERT INTO ... VALUES ... statement, then
 *              this stores values to be inserted. Otherwise NULL.
 * pIdList   -> If this is an INSERT INTO ... (<column-names>) VALUES ... 
 *              statement, then this stores the column-names to be
 *              inserted into.
 *
 * (op == TK_DELETE)
 * target    -> A token holding the quoted name of the table to delete from.
 * pWhere    -> The WHERE clause of the DELETE statement if one is specified.
 *              Otherwise NULL.
 * 
 * (op == TK_UPDATE)
 * target    -> A token holding the quoted name of the table to update rows of.
 * pWhere    -> The WHERE clause of the UPDATE statement if one is specified.
 *              Otherwise NULL.
 * pExprList -> A list of the columns to update and the expressions to update
 *              them to. See sqlite3Update() documentation of "pChanges"
 *              argument.
 * 
 */
struct TriggerStep {
  u8 op;               /* One of TK_DELETE, TK_UPDATE, TK_INSERT, TK_SELECT */
  u8 orconf;           /* OE_Rollback etc. */
  Trigger *pTrig;      /* The trigger that this step is a part of */
  Select *pSelect;     /* SELECT statment or RHS of INSERT INTO .. SELECT ... */
  Token target;        /* Target table for DELETE, UPDATE, INSERT */
  Expr *pWhere;        /* The WHERE clause for DELETE or UPDATE steps */
  ExprList *pExprList; /* SET clause for UPDATE.  VALUES clause for INSERT */
  IdList *pIdList;     /* Column names for INSERT */
  TriggerStep *pNext;  /* Next in the link-list */
  TriggerStep *pLast;  /* Last element in link-list. Valid for 1st elem only */
};

/*
** The following structure contains information used by the sqliteFix...
** routines as they walk the parse tree to make database references
** explicit.  
*/
typedef struct DbFixer DbFixer;
struct DbFixer {
  Parse *pParse;      /* The parsing context.  Error messages written here */
  const char *zDb;    /* Make sure all objects are contained in this database */
  const char *zType;  /* Type of the container - used for error messages */
  const Token *pName; /* Name of the container - used for error messages */
};

/*
** An objected used to accumulate the text of a string where we
** do not necessarily know how big the string will be in the end.
*/
struct StrAccum {
  sqlite3 *db;         /* Optional database for lookaside.  Can be NULL */
  char *zBase;         /* A base allocation.  Not from malloc. */
  char *zText;         /* The string collected so far */
  int  nChar;          /* Length of the string so far */
  int  nAlloc;         /* Amount of space allocated in zText */
  int  mxAlloc;        /* Maximum allowed string length */
  u8   mallocFailed;   /* Becomes true if any memory allocation fails */
  u8   useMalloc;      /* 0: none,  1: sqlite3DbMalloc,  2: sqlite3_malloc */
  u8   tooBig;         /* Becomes true if string size exceeds limits */
};

/*
** A pointer to this structure is used to communicate information
** from sqlite3Init and OP_ParseSchema into the sqlite3InitCallback.
*/
typedef struct {
  sqlite3 *db;        /* The database being initialized */
  int iDb;            /* 0 for main database.  1 for TEMP, 2.. for ATTACHed */
  char **pzErrMsg;    /* Error message stored here */
  int rc;             /* Result code stored here */
} InitData;

/*
** Structure containing global configuration data for the SQLite library.
**
** This structure also contains some state information.
*/
struct Sqlite3Config {
  int bMemstat;                     /* True to enable memory status */
  int bCoreMutex;                   /* True to enable core mutexing */
  int bFullMutex;                   /* True to enable full mutexing */
  int mxStrlen;                     /* Maximum string length */
  int szLookaside;                  /* Default lookaside buffer size */
  int nLookaside;                   /* Default lookaside buffer count */
  sqlite3_mem_methods m;            /* Low-level memory allocation interface */
  sqlite3_mutex_methods mutex;      /* Low-level mutex interface */
  sqlite3_pcache_methods pcache;    /* Low-level page-cache interface */
  void *pHeap;                      /* Heap storage space */
  int nHeap;                        /* Size of pHeap[] */
  int mnReq, mxReq;                 /* Min and max heap requests sizes */
  void *pScratch;                   /* Scratch memory */
  int szScratch;                    /* Size of each scratch buffer */
  int nScratch;                     /* Number of scratch buffers */
  void *pPage;                      /* Page cache memory */
  int szPage;                       /* Size of each page in pPage[] */
  int nPage;                        /* Number of pages in pPage[] */
  int mxParserStack;                /* maximum depth of the parser stack */
  int sharedCacheEnabled;           /* true if shared-cache mode enabled */
  /* The above might be initialized to non-zero.  The following need to always
  ** initially be zero, however. */
  int isInit;                       /* True after initialization has finished */
  int inProgress;                   /* True while initialization in progress */
  int isMutexInit;                  /* True after mutexes are initialized */
  int isMallocInit;                 /* True after malloc is initialized */
  int isPCacheInit;                 /* True after malloc is initialized */
  sqlite3_mutex *pInitMutex;        /* Mutex used by sqlite3_initialize() */
  int nRefInitMutex;                /* Number of users of pInitMutex */
  void (*xLog)(void*,int,const char*); /* Function for logging */
  void *pLogArg;                       /* First argument to xLog() */
};

/*
** Context pointer passed down through the tree-walk.
*/
struct Walker {
  int (*xExprCallback)(Walker*, Expr*);     /* Callback for expressions */
  int (*xSelectCallback)(Walker*,Select*);  /* Callback for SELECTs */
  Parse *pParse;                            /* Parser context.  */
  union {                                   /* Extra data for callback */
    NameContext *pNC;                          /* Naming context */
    int i;                                     /* Integer value */
  } u;
};

/* Forward declarations */
SQLITE_PRIVATE int sqlite3WalkExpr(Walker*, Expr*);
SQLITE_PRIVATE int sqlite3WalkExprList(Walker*, ExprList*);
SQLITE_PRIVATE int sqlite3WalkSelect(Walker*, Select*);
SQLITE_PRIVATE int sqlite3WalkSelectExpr(Walker*, Select*);
SQLITE_PRIVATE int sqlite3WalkSelectFrom(Walker*, Select*);

/*
** Return code from the parse-tree walking primitives and their
** callbacks.
*/
#define WRC_Continue    0   /* Continue down into children */
#define WRC_Prune       1   /* Omit children but continue walking siblings */
#define WRC_Abort       2   /* Abandon the tree walk */

/*
** Assuming zIn points to the first byte of a UTF-8 character,
** advance zIn to point to the first byte of the next UTF-8 character.
*/
#define SQLITE_SKIP_UTF8(zIn) {                        \
  if( (*(zIn++))>=0xc0 ){                              \
    while( (*zIn & 0xc0)==0x80 ){ zIn++; }             \
  }                                                    \
}

/*
** The SQLITE_*_BKPT macros are substitutes for the error codes with
** the same name but without the _BKPT suffix.  These macros invoke
** routines that report the line-number on which the error originated
** using sqlite3_log().  The routines also provide a convenient place
** to set a debugger breakpoint.
*/
SQLITE_PRIVATE int sqlite3CorruptError(int);
SQLITE_PRIVATE int sqlite3MisuseError(int);
SQLITE_PRIVATE int sqlite3CantopenError(int);
#define SQLITE_CORRUPT_BKPT sqlite3CorruptError(__LINE__)
#define SQLITE_MISUSE_BKPT sqlite3MisuseError(__LINE__)
#define SQLITE_CANTOPEN_BKPT sqlite3CantopenError(__LINE__)


/*
** FTS4 is really an extension for FTS3.  It is enabled using the
** SQLITE_ENABLE_FTS3 macro.  But to avoid confusion we also all
** the SQLITE_ENABLE_FTS4 macro to serve as an alisse for SQLITE_ENABLE_FTS3.
*/
#if defined(SQLITE_ENABLE_FTS4) && !defined(SQLITE_ENABLE_FTS3)
# define SQLITE_ENABLE_FTS3
#endif

/*
** The ctype.h header is needed for non-ASCII systems.  It is also
** needed by FTS3 when FTS3 is included in the amalgamation.
*/
#if !defined(SQLITE_ASCII) || \
    (defined(SQLITE_ENABLE_FTS3) && defined(SQLITE_AMALGAMATION))
//# include <ctype.h>
#endif

/*
** The following macros mimic the standard library functions toupper(),
** isspace(), isalnum(), isdigit() and isxdigit(), respectively. The
** sqlite versions only work for ASCII characters, regardless of locale.
*/
#ifdef SQLITE_ASCII
# define sqlite3Toupper(x)  ((x)&~(sqlite3CtypeMap[(unsigned char)(x)]&0x20))
# define sqlite3Isspace(x)   (sqlite3CtypeMap[(unsigned char)(x)]&0x01)
# define sqlite3Isalnum(x)   (sqlite3CtypeMap[(unsigned char)(x)]&0x06)
# define sqlite3Isalpha(x)   (sqlite3CtypeMap[(unsigned char)(x)]&0x02)
# define sqlite3Isdigit(x)   (sqlite3CtypeMap[(unsigned char)(x)]&0x04)
# define sqlite3Isxdigit(x)  (sqlite3CtypeMap[(unsigned char)(x)]&0x08)
# define sqlite3Tolower(x)   (sqlite3UpperToLower[(unsigned char)(x)])
#else
# define sqlite3Toupper(x)   toupper((unsigned char)(x))
# define sqlite3Isspace(x)   isspace((unsigned char)(x))
# define sqlite3Isalnum(x)   isalnum((unsigned char)(x))
# define sqlite3Isalpha(x)   isalpha((unsigned char)(x))
# define sqlite3Isdigit(x)   isdigit((unsigned char)(x))
# define sqlite3Isxdigit(x)  isxdigit((unsigned char)(x))
# define sqlite3Tolower(x)   tolower((unsigned char)(x))
#endif

/*
** Internal function prototypes
*/
SQLITE_PRIVATE int sqlite3StrICmp(const char *, const char *);
SQLITE_PRIVATE int sqlite3Strlen30(const char*);
#define sqlite3StrNICmp sqlite3_strnicmp

SQLITE_PRIVATE int sqlite3MallocInit(void);
SQLITE_PRIVATE void sqlite3MallocEnd(void);
SQLITE_PRIVATE void *sqlite3Malloc(int);
SQLITE_PRIVATE void *sqlite3MallocZero(int);
SQLITE_PRIVATE void *sqlite3DbMallocZero(sqlite3*, int);
SQLITE_PRIVATE void *sqlite3DbMallocRaw(sqlite3*, int);
SQLITE_PRIVATE char *sqlite3DbStrDup(sqlite3*,const char*);
SQLITE_PRIVATE char *sqlite3DbStrNDup(sqlite3*,const char*, int);
SQLITE_PRIVATE void *sqlite3Realloc(void*, int);
SQLITE_PRIVATE void *sqlite3DbReallocOrFree(sqlite3 *, void *, int);
SQLITE_PRIVATE void *sqlite3DbRealloc(sqlite3 *, void *, int);
SQLITE_PRIVATE void sqlite3DbFree(sqlite3*, void*);
SQLITE_PRIVATE int sqlite3MallocSize(void*);
SQLITE_PRIVATE int sqlite3DbMallocSize(sqlite3*, void*);
SQLITE_PRIVATE void *sqlite3ScratchMalloc(int);
SQLITE_PRIVATE void sqlite3ScratchFree(void*);
SQLITE_PRIVATE void *sqlite3PageMalloc(int);
SQLITE_PRIVATE void sqlite3PageFree(void*);
SQLITE_PRIVATE void sqlite3MemSetDefault(void);
SQLITE_PRIVATE void sqlite3BenignMallocHooks(void (*)(void), void (*)(void));
SQLITE_PRIVATE int sqlite3HeapNearlyFull(void);

/*
** On systems with ample stack space and that support alloca(), make
** use of alloca() to obtain space for large automatic objects.  By default,
** obtain space from malloc().
**
** The alloca() routine never returns NULL.  This will cause code paths
** that deal with sqlite3StackAlloc() failures to be unreachable.
*/
#ifdef SQLITE_USE_ALLOCA
# define sqlite3StackAllocRaw(D,N)   alloca(N)
# define sqlite3StackAllocZero(D,N)  memset(alloca(N), 0, N)
# define sqlite3StackFree(D,P)       
#else
# define sqlite3StackAllocRaw(D,N)   sqlite3DbMallocRaw(D,N)
# define sqlite3StackAllocZero(D,N)  sqlite3DbMallocZero(D,N)
# define sqlite3StackFree(D,P)       sqlite3DbFree(D,P)
#endif

#ifdef SQLITE_ENABLE_MEMSYS3
SQLITE_PRIVATE const sqlite3_mem_methods *sqlite3MemGetMemsys3(void);
#endif
#ifdef SQLITE_ENABLE_MEMSYS5
SQLITE_PRIVATE const sqlite3_mem_methods *sqlite3MemGetMemsys5(void);
#endif


#ifndef SQLITE_MUTEX_OMIT
SQLITE_PRIVATE   sqlite3_mutex_methods const *sqlite3DefaultMutex(void);
SQLITE_PRIVATE   sqlite3_mutex_methods const *sqlite3NoopMutex(void);
SQLITE_PRIVATE   sqlite3_mutex *sqlite3MutexAlloc(int);
SQLITE_PRIVATE   int sqlite3MutexInit(void);
SQLITE_PRIVATE   int sqlite3MutexEnd(void);
#endif

SQLITE_PRIVATE int sqlite3StatusValue(int);
SQLITE_PRIVATE void sqlite3StatusAdd(int, int);
SQLITE_PRIVATE void sqlite3StatusSet(int, int);

#ifndef SQLITE_OMIT_FLOATING_POINT
SQLITE_PRIVATE   int sqlite3IsNaN(double);
#else
# define sqlite3IsNaN(X)  0
#endif

SQLITE_PRIVATE void sqlite3VXPrintf(StrAccum*, int, const char*, va_list);
#ifndef SQLITE_OMIT_TRACE
SQLITE_PRIVATE void sqlite3XPrintf(StrAccum*, const char*, ...);
#endif
SQLITE_PRIVATE char *sqlite3MPrintf(sqlite3*,const char*, ...);
SQLITE_PRIVATE char *sqlite3VMPrintf(sqlite3*,const char*, va_list);
SQLITE_PRIVATE char *sqlite3MAppendf(sqlite3*,char*,const char*,...);
#if (defined(SQLITE_TEST) || defined(SQLITE_DEBUG))
SQLITE_PRIVATE   void sqlite3DebugPrintf(const char*, ...);
#endif
#if defined(SQLITE_TEST)
SQLITE_PRIVATE   void *sqlite3TestTextToPtr(const char*);
#endif
SQLITE_PRIVATE void sqlite3SetString(char **, sqlite3*, const char*, ...);
SQLITE_PRIVATE void sqlite3ErrorMsg(Parse*, const char*, ...);
SQLITE_PRIVATE int sqlite3Dequote(char*);
SQLITE_PRIVATE int sqlite3KeywordCode(const unsigned char*, int);
SQLITE_PRIVATE int sqlite3RunParser(Parse*, const char*, char **);
SQLITE_PRIVATE void sqlite3FinishCoding(Parse*);
SQLITE_PRIVATE int sqlite3GetTempReg(Parse*);
SQLITE_PRIVATE void sqlite3ReleaseTempReg(Parse*,int);
SQLITE_PRIVATE int sqlite3GetTempRange(Parse*,int);
SQLITE_PRIVATE void sqlite3ReleaseTempRange(Parse*,int,int);
SQLITE_PRIVATE Expr *sqlite3ExprAlloc(sqlite3*,int,const Token*,int);
SQLITE_PRIVATE Expr *sqlite3Expr(sqlite3*,int,const char*);
SQLITE_PRIVATE void sqlite3ExprAttachSubtrees(sqlite3*,Expr*,Expr*,Expr*);
SQLITE_PRIVATE Expr *sqlite3PExpr(Parse*, int, Expr*, Expr*, const Token*);
SQLITE_PRIVATE Expr *sqlite3ExprAnd(sqlite3*,Expr*, Expr*);
SQLITE_PRIVATE Expr *sqlite3ExprFunction(Parse*,ExprList*, Token*);
SQLITE_PRIVATE void sqlite3ExprAssignVarNumber(Parse*, Expr*);
SQLITE_PRIVATE void sqlite3ExprDelete(sqlite3*, Expr*);
SQLITE_PRIVATE ExprList *sqlite3ExprListAppend(Parse*,ExprList*,Expr*);
SQLITE_PRIVATE void sqlite3ExprListSetName(Parse*,ExprList*,Token*,int);
SQLITE_PRIVATE void sqlite3ExprListSetSpan(Parse*,ExprList*,ExprSpan*);
SQLITE_PRIVATE void sqlite3ExprListDelete(sqlite3*, ExprList*);
SQLITE_PRIVATE int sqlite3Init(sqlite3*, char**);
SQLITE_PRIVATE int sqlite3InitCallback(void*, int, char**, char**);
SQLITE_PRIVATE void sqlite3Pragma(Parse*,Token*,Token*,Token*,int);
SQLITE_PRIVATE void sqlite3ResetInternalSchema(sqlite3*, int);
SQLITE_PRIVATE void sqlite3BeginParse(Parse*,int);
SQLITE_PRIVATE void sqlite3CommitInternalChanges(sqlite3*);
SQLITE_PRIVATE Table *sqlite3ResultSetOfSelect(Parse*,Select*);
SQLITE_PRIVATE void sqlite3OpenMasterTable(Parse *, int);
SQLITE_PRIVATE void sqlite3StartTable(Parse*,Token*,Token*,int,int,int,int);
SQLITE_PRIVATE void sqlite3AddColumn(Parse*,Token*);
SQLITE_PRIVATE void sqlite3AddNotNull(Parse*, int);
SQLITE_PRIVATE void sqlite3AddPrimaryKey(Parse*, ExprList*, int, int, int);
SQLITE_PRIVATE void sqlite3AddCheckConstraint(Parse*, Expr*);
SQLITE_PRIVATE void sqlite3AddColumnType(Parse*,Token*);
SQLITE_PRIVATE void sqlite3AddDefaultValue(Parse*,ExprSpan*);
SQLITE_PRIVATE void sqlite3AddCollateType(Parse*, Token*);
SQLITE_PRIVATE void sqlite3EndTable(Parse*,Token*,Token*,Select*);

SQLITE_PRIVATE Bitvec *sqlite3BitvecCreate(u32);
SQLITE_PRIVATE int sqlite3BitvecTest(Bitvec*, u32);
SQLITE_PRIVATE int sqlite3BitvecSet(Bitvec*, u32);
SQLITE_PRIVATE void sqlite3BitvecClear(Bitvec*, u32, void*);
SQLITE_PRIVATE void sqlite3BitvecDestroy(Bitvec*);
SQLITE_PRIVATE u32 sqlite3BitvecSize(Bitvec*);
SQLITE_PRIVATE int sqlite3BitvecBuiltinTest(int,int*);

SQLITE_PRIVATE RowSet *sqlite3RowSetInit(sqlite3*, void*, unsigned int);
SQLITE_PRIVATE void sqlite3RowSetClear(RowSet*);
SQLITE_PRIVATE void sqlite3RowSetInsert(RowSet*, i64);
SQLITE_PRIVATE int sqlite3RowSetTest(RowSet*, u8 iBatch, i64);
SQLITE_PRIVATE int sqlite3RowSetNext(RowSet*, i64*);

SQLITE_PRIVATE void sqlite3CreateView(Parse*,Token*,Token*,Token*,Select*,int,int);

#if !defined(SQLITE_OMIT_VIEW) || !defined(SQLITE_OMIT_VIRTUALTABLE)
SQLITE_PRIVATE   int sqlite3ViewGetColumnNames(Parse*,Table*);
#else
# define sqlite3ViewGetColumnNames(A,B) 0
#endif

SQLITE_PRIVATE void sqlite3DropTable(Parse*, SrcList*, int, int);
SQLITE_PRIVATE void sqlite3DeleteTable(sqlite3*, Table*);
#ifndef SQLITE_OMIT_AUTOINCREMENT
SQLITE_PRIVATE   void sqlite3AutoincrementBegin(Parse *pParse);
SQLITE_PRIVATE   void sqlite3AutoincrementEnd(Parse *pParse);
#else
# define sqlite3AutoincrementBegin(X)
# define sqlite3AutoincrementEnd(X)
#endif
SQLITE_PRIVATE void sqlite3Insert(Parse*, SrcList*, ExprList*, Select*, IdList*, int);
SQLITE_PRIVATE void *sqlite3ArrayAllocate(sqlite3*,void*,int,int,int*,int*,int*);
SQLITE_PRIVATE IdList *sqlite3IdListAppend(sqlite3*, IdList*, Token*);
SQLITE_PRIVATE int sqlite3IdListIndex(IdList*,const char*);
SQLITE_PRIVATE SrcList *sqlite3SrcListEnlarge(sqlite3*, SrcList*, int, int);
SQLITE_PRIVATE SrcList *sqlite3SrcListAppend(sqlite3*, SrcList*, Token*, Token*);
SQLITE_PRIVATE SrcList *sqlite3SrcListAppendFromTerm(Parse*, SrcList*, Token*, Token*,
                                      Token*, Select*, Expr*, IdList*);
SQLITE_PRIVATE void sqlite3SrcListIndexedBy(Parse *, SrcList *, Token *);
SQLITE_PRIVATE int sqlite3IndexedByLookup(Parse *, struct SrcList_item *);
SQLITE_PRIVATE void sqlite3SrcListShiftJoinType(SrcList*);
SQLITE_PRIVATE void sqlite3SrcListAssignCursors(Parse*, SrcList*);
SQLITE_PRIVATE void sqlite3IdListDelete(sqlite3*, IdList*);
SQLITE_PRIVATE void sqlite3SrcListDelete(sqlite3*, SrcList*);
SQLITE_PRIVATE Index *sqlite3CreateIndex(Parse*,Token*,Token*,SrcList*,ExprList*,int,Token*,
                        Token*, int, int);
SQLITE_PRIVATE void sqlite3DropIndex(Parse*, SrcList*, int);
SQLITE_PRIVATE int sqlite3Select(Parse*, Select*, SelectDest*);
SQLITE_PRIVATE Select *sqlite3SelectNew(Parse*,ExprList*,SrcList*,Expr*,ExprList*,
                         Expr*,ExprList*,int,Expr*,Expr*);
SQLITE_PRIVATE void sqlite3SelectDelete(sqlite3*, Select*);
SQLITE_PRIVATE Table *sqlite3SrcListLookup(Parse*, SrcList*);
SQLITE_PRIVATE int sqlite3IsReadOnly(Parse*, Table*, int);
SQLITE_PRIVATE void sqlite3OpenTable(Parse*, int iCur, int iDb, Table*, int);
#if defined(SQLITE_ENABLE_UPDATE_DELETE_LIMIT) && !defined(SQLITE_OMIT_SUBQUERY)
SQLITE_PRIVATE Expr *sqlite3LimitWhere(Parse *, SrcList *, Expr *, ExprList *, Expr *, Expr *, char *);
#endif
SQLITE_PRIVATE void sqlite3DeleteFrom(Parse*, SrcList*, Expr*);
SQLITE_PRIVATE void sqlite3Update(Parse*, SrcList*, ExprList*, Expr*, int);
SQLITE_PRIVATE WhereInfo *sqlite3WhereBegin(Parse*, SrcList*, Expr*, ExprList**, u16);
SQLITE_PRIVATE void sqlite3WhereEnd(WhereInfo*);
SQLITE_PRIVATE int sqlite3ExprCodeGetColumn(Parse*, Table*, int, int, int);
SQLITE_PRIVATE void sqlite3ExprCodeGetColumnOfTable(Vdbe*, Table*, int, int, int);
SQLITE_PRIVATE void sqlite3ExprCodeMove(Parse*, int, int, int);
SQLITE_PRIVATE void sqlite3ExprCodeCopy(Parse*, int, int, int);
SQLITE_PRIVATE void sqlite3ExprCacheStore(Parse*, int, int, int);
SQLITE_PRIVATE void sqlite3ExprCachePush(Parse*);
SQLITE_PRIVATE void sqlite3ExprCachePop(Parse*, int);
SQLITE_PRIVATE void sqlite3ExprCacheRemove(Parse*, int, int);
SQLITE_PRIVATE void sqlite3ExprCacheClear(Parse*);
SQLITE_PRIVATE void sqlite3ExprCacheAffinityChange(Parse*, int, int);
SQLITE_PRIVATE int sqlite3ExprCode(Parse*, Expr*, int);
SQLITE_PRIVATE int sqlite3ExprCodeTemp(Parse*, Expr*, int*);
SQLITE_PRIVATE int sqlite3ExprCodeTarget(Parse*, Expr*, int);
SQLITE_PRIVATE int sqlite3ExprCodeAndCache(Parse*, Expr*, int);
SQLITE_PRIVATE void sqlite3ExprCodeConstants(Parse*, Expr*);
SQLITE_PRIVATE int sqlite3ExprCodeExprList(Parse*, ExprList*, int, int);
SQLITE_PRIVATE void sqlite3ExprIfTrue(Parse*, Expr*, int, int);
SQLITE_PRIVATE void sqlite3ExprIfFalse(Parse*, Expr*, int, int);
SQLITE_PRIVATE Table *sqlite3FindTable(sqlite3*,const char*, const char*);
SQLITE_PRIVATE Table *sqlite3LocateTable(Parse*,int isView,const char*, const char*);
SQLITE_PRIVATE Index *sqlite3FindIndex(sqlite3*,const char*, const char*);
SQLITE_PRIVATE void sqlite3UnlinkAndDeleteTable(sqlite3*,int,const char*);
SQLITE_PRIVATE void sqlite3UnlinkAndDeleteIndex(sqlite3*,int,const char*);
SQLITE_PRIVATE void sqlite3Vacuum(Parse*);
SQLITE_PRIVATE int sqlite3RunVacuum(char**, sqlite3*);
SQLITE_PRIVATE char *sqlite3NameFromToken(sqlite3*, Token*);
SQLITE_PRIVATE int sqlite3ExprCompare(Expr*, Expr*);
SQLITE_PRIVATE int sqlite3ExprListCompare(ExprList*, ExprList*);
SQLITE_PRIVATE void sqlite3ExprAnalyzeAggregates(NameContext*, Expr*);
SQLITE_PRIVATE void sqlite3ExprAnalyzeAggList(NameContext*,ExprList*);
SQLITE_PRIVATE Vdbe *sqlite3GetVdbe(Parse*);
SQLITE_PRIVATE void sqlite3PrngSaveState(void);
SQLITE_PRIVATE void sqlite3PrngRestoreState(void);
SQLITE_PRIVATE void sqlite3PrngResetState(void);
SQLITE_PRIVATE void sqlite3RollbackAll(sqlite3*);
SQLITE_PRIVATE void sqlite3CodeVerifySchema(Parse*, int);
SQLITE_PRIVATE void sqlite3CodeVerifyNamedSchema(Parse*, const char *zDb);
SQLITE_PRIVATE void sqlite3BeginTransaction(Parse*, int);
SQLITE_PRIVATE void sqlite3CommitTransaction(Parse*);
SQLITE_PRIVATE void sqlite3RollbackTransaction(Parse*);
SQLITE_PRIVATE void sqlite3Savepoint(Parse*, int, Token*);
SQLITE_PRIVATE void sqlite3CloseSavepoints(sqlite3 *);
SQLITE_PRIVATE int sqlite3ExprIsConstant(Expr*);
SQLITE_PRIVATE int sqlite3ExprIsConstantNotJoin(Expr*);
SQLITE_PRIVATE int sqlite3ExprIsConstantOrFunction(Expr*);
SQLITE_PRIVATE int sqlite3ExprIsInteger(Expr*, int*);
SQLITE_PRIVATE int sqlite3ExprCanBeNull(const Expr*);
SQLITE_PRIVATE void sqlite3ExprCodeIsNullJump(Vdbe*, const Expr*, int, int);
SQLITE_PRIVATE int sqlite3ExprNeedsNoAffinityChange(const Expr*, char);
SQLITE_PRIVATE int sqlite3IsRowid(const char*);
SQLITE_PRIVATE void sqlite3GenerateRowDelete(Parse*, Table*, int, int, int, Trigger *, int);
SQLITE_PRIVATE void sqlite3GenerateRowIndexDelete(Parse*, Table*, int, int*);
SQLITE_PRIVATE int sqlite3GenerateIndexKey(Parse*, Index*, int, int, int);
SQLITE_PRIVATE void sqlite3GenerateConstraintChecks(Parse*,Table*,int,int,
                                     int*,int,int,int,int,int*);
SQLITE_PRIVATE void sqlite3CompleteInsertion(Parse*, Table*, int, int, int*, int, int, int);
SQLITE_PRIVATE int sqlite3OpenTableAndIndices(Parse*, Table*, int, int);
SQLITE_PRIVATE void sqlite3BeginWriteOperation(Parse*, int, int);
SQLITE_PRIVATE void sqlite3MultiWrite(Parse*);
SQLITE_PRIVATE void sqlite3MayAbort(Parse*);
SQLITE_PRIVATE void sqlite3HaltConstraint(Parse*, int, char*, int);
SQLITE_PRIVATE Expr *sqlite3ExprDup(sqlite3*,Expr*,int);
SQLITE_PRIVATE ExprList *sqlite3ExprListDup(sqlite3*,ExprList*,int);
SQLITE_PRIVATE SrcList *sqlite3SrcListDup(sqlite3*,SrcList*,int);
SQLITE_PRIVATE IdList *sqlite3IdListDup(sqlite3*,IdList*);
SQLITE_PRIVATE Select *sqlite3SelectDup(sqlite3*,Select*,int);
SQLITE_PRIVATE void sqlite3FuncDefInsert(FuncDefHash*, FuncDef*);
SQLITE_PRIVATE FuncDef *sqlite3FindFunction(sqlite3*,const char*,int,int,u8,int);
SQLITE_PRIVATE void sqlite3RegisterBuiltinFunctions(sqlite3*);
SQLITE_PRIVATE void sqlite3RegisterDateTimeFunctions(void);
SQLITE_PRIVATE void sqlite3RegisterGlobalFunctions(void);
SQLITE_PRIVATE int sqlite3SafetyCheckOk(sqlite3*);
SQLITE_PRIVATE int sqlite3SafetyCheckSickOrOk(sqlite3*);
SQLITE_PRIVATE void sqlite3ChangeCookie(Parse*, int);

#if !defined(SQLITE_OMIT_VIEW) && !defined(SQLITE_OMIT_TRIGGER)
SQLITE_PRIVATE void sqlite3MaterializeView(Parse*, Table*, Expr*, int);
#endif

#ifndef SQLITE_OMIT_TRIGGER
SQLITE_PRIVATE   void sqlite3BeginTrigger(Parse*, Token*,Token*,int,int,IdList*,SrcList*,
                           Expr*,int, int);
SQLITE_PRIVATE   void sqlite3FinishTrigger(Parse*, TriggerStep*, Token*);
SQLITE_PRIVATE   void sqlite3DropTrigger(Parse*, SrcList*, int);
SQLITE_PRIVATE   void sqlite3DropTriggerPtr(Parse*, Trigger*);
SQLITE_PRIVATE   Trigger *sqlite3TriggersExist(Parse *, Table*, int, ExprList*, int *pMask);
SQLITE_PRIVATE   Trigger *sqlite3TriggerList(Parse *, Table *);
SQLITE_PRIVATE   void sqlite3CodeRowTrigger(Parse*, Trigger *, int, ExprList*, int, Table *,
                            int, int, int);
SQLITE_PRIVATE   void sqlite3CodeRowTriggerDirect(Parse *, Trigger *, Table *, int, int, int);
  void sqliteViewTriggers(Parse*, Table*, Expr*, int, ExprList*);
SQLITE_PRIVATE   void sqlite3DeleteTriggerStep(sqlite3*, TriggerStep*);
SQLITE_PRIVATE   TriggerStep *sqlite3TriggerSelectStep(sqlite3*,Select*);
SQLITE_PRIVATE   TriggerStep *sqlite3TriggerInsertStep(sqlite3*,Token*, IdList*,
                                        ExprList*,Select*,u8);
SQLITE_PRIVATE   TriggerStep *sqlite3TriggerUpdateStep(sqlite3*,Token*,ExprList*, Expr*, u8);
SQLITE_PRIVATE   TriggerStep *sqlite3TriggerDeleteStep(sqlite3*,Token*, Expr*);
SQLITE_PRIVATE   void sqlite3DeleteTrigger(sqlite3*, Trigger*);
SQLITE_PRIVATE   void sqlite3UnlinkAndDeleteTrigger(sqlite3*,int,const char*);
SQLITE_PRIVATE   u32 sqlite3TriggerColmask(Parse*,Trigger*,ExprList*,int,int,Table*,int);
# define sqlite3ParseToplevel(p) ((p)->pToplevel ? (p)->pToplevel : (p))
#else
# define sqlite3TriggersExist(B,C,D,E,F) 0
# define sqlite3DeleteTrigger(A,B)
# define sqlite3DropTriggerPtr(A,B)
# define sqlite3UnlinkAndDeleteTrigger(A,B,C)
# define sqlite3CodeRowTrigger(A,B,C,D,E,F,G,H,I)
# define sqlite3CodeRowTriggerDirect(A,B,C,D,E,F)
# define sqlite3TriggerList(X, Y) 0
# define sqlite3ParseToplevel(p) p
# define sqlite3TriggerColmask(A,B,C,D,E,F,G) 0
#endif

SQLITE_PRIVATE int sqlite3JoinType(Parse*, Token*, Token*, Token*);
SQLITE_PRIVATE void sqlite3CreateForeignKey(Parse*, ExprList*, Token*, ExprList*, int);
SQLITE_PRIVATE void sqlite3DeferForeignKey(Parse*, int);
# define sqlite3AuthRead(a,b,c,d)
# define sqlite3AuthCheck(a,b,c,d,e)    SQLITE_OK
# define sqlite3AuthContextPush(a,b,c)
# define sqlite3AuthContextPop(a)  ((void)(a))
SQLITE_PRIVATE void sqlite3Attach(Parse*, Expr*, Expr*, Expr*);
SQLITE_PRIVATE void sqlite3Detach(Parse*, Expr*);
SQLITE_PRIVATE int sqlite3FixInit(DbFixer*, Parse*, int, const char*, const Token*);
SQLITE_PRIVATE int sqlite3FixSrcList(DbFixer*, SrcList*);
SQLITE_PRIVATE int sqlite3FixSelect(DbFixer*, Select*);
SQLITE_PRIVATE int sqlite3FixExpr(DbFixer*, Expr*);
SQLITE_PRIVATE int sqlite3FixExprList(DbFixer*, ExprList*);
SQLITE_PRIVATE int sqlite3FixTriggerStep(DbFixer*, TriggerStep*);
SQLITE_PRIVATE int sqlite3AtoF(const char *z, double*, int, u8);
SQLITE_PRIVATE int sqlite3GetInt32(const char *, int*);
SQLITE_PRIVATE int sqlite3Atoi(const char*);
SQLITE_PRIVATE int sqlite3Utf16ByteLen(const void *pData, int nChar);
SQLITE_PRIVATE int sqlite3Utf8CharLen(const char *pData, int nByte);
SQLITE_PRIVATE int sqlite3Utf8Read(const u8*, const u8**);

/*
** Routines to read and write variable-length integers.  These used to
** be defined locally, but now we use the varint routines in the util.c
** file.  Code should use the MACRO forms below, as the Varint32 versions
** are coded to assume the single byte case is already handled (which 
** the MACRO form does).
*/
SQLITE_PRIVATE int sqlite3PutVarint(unsigned char*, u64);
SQLITE_PRIVATE int sqlite3PutVarint32(unsigned char*, u32);
SQLITE_PRIVATE u8 sqlite3GetVarint(const unsigned char *, u64 *);
SQLITE_PRIVATE u8 sqlite3GetVarint32(const unsigned char *, u32 *);
SQLITE_PRIVATE int sqlite3VarintLen(u64 v);

/*
** The header of a record consists of a sequence variable-length integers.
** These integers are almost always small and are encoded as a single byte.
** The following macros take advantage this fact to provide a fast encode
** and decode of the integers in a record header.  It is faster for the common
** case where the integer is a single byte.  It is a little slower when the
** integer is two or more bytes.  But overall it is faster.
**
** The following expressions are equivalent:
**
**     x = sqlite3GetVarint32( A, &B );
**     x = sqlite3PutVarint32( A, B );
**
**     x = getVarint32( A, B );
**     x = putVarint32( A, B );
**
*/
#define getVarint32(A,B)  (u8)((*(A)<(u8)0x80) ? ((B) = (u32)*(A)),1 : sqlite3GetVarint32((A), (u32 *)&(B)))
#define putVarint32(A,B)  (u8)(((u32)(B)<(u32)0x80) ? (*(A) = (unsigned char)(B)),1 : sqlite3PutVarint32((A), (B)))
#define getVarint    sqlite3GetVarint
#define putVarint    sqlite3PutVarint


SQLITE_PRIVATE const char *sqlite3IndexAffinityStr(Vdbe *, Index *);
SQLITE_PRIVATE void sqlite3TableAffinityStr(Vdbe *, Table *);
SQLITE_PRIVATE char sqlite3CompareAffinity(Expr *pExpr, char aff2);
SQLITE_PRIVATE int sqlite3IndexAffinityOk(Expr *pExpr, char idx_affinity);
SQLITE_PRIVATE char sqlite3ExprAffinity(Expr *pExpr);
SQLITE_PRIVATE int sqlite3Atoi64(const char*, i64*, int, u8);
SQLITE_PRIVATE void sqlite3Error(sqlite3*, int, const char*,...);
SQLITE_PRIVATE void *sqlite3HexToBlob(sqlite3*, const char *z, int n);
SQLITE_PRIVATE int sqlite3TwoPartName(Parse *, Token *, Token *, Token **);
SQLITE_PRIVATE const char *sqlite3ErrStr(int);
SQLITE_PRIVATE int sqlite3ReadSchema(Parse *pParse);
SQLITE_PRIVATE CollSeq *sqlite3FindCollSeq(sqlite3*,u8 enc, const char*,int);
SQLITE_PRIVATE CollSeq *sqlite3LocateCollSeq(Parse *pParse, const char*zName);
SQLITE_PRIVATE CollSeq *sqlite3ExprCollSeq(Parse *pParse, Expr *pExpr);
SQLITE_PRIVATE Expr *sqlite3ExprSetColl(Expr*, CollSeq*);
SQLITE_PRIVATE Expr *sqlite3ExprSetCollByToken(Parse *pParse, Expr*, Token*);
SQLITE_PRIVATE int sqlite3CheckCollSeq(Parse *, CollSeq *);
SQLITE_PRIVATE int sqlite3CheckObjectName(Parse *, const char *);
SQLITE_PRIVATE void sqlite3VdbeSetChanges(sqlite3 *, int);
SQLITE_PRIVATE int sqlite3AddInt64(i64*,i64);
SQLITE_PRIVATE int sqlite3SubInt64(i64*,i64);
SQLITE_PRIVATE int sqlite3MulInt64(i64*,i64);
SQLITE_PRIVATE int sqlite3AbsInt32(int);

SQLITE_PRIVATE const void *sqlite3ValueText(sqlite3_value*, u8);
SQLITE_PRIVATE int sqlite3ValueBytes(sqlite3_value*, u8);
SQLITE_PRIVATE void sqlite3ValueSetStr(sqlite3_value*, int, const void *,u8, 
                        void(*)(void*));
SQLITE_PRIVATE void sqlite3ValueFree(sqlite3_value*);
SQLITE_PRIVATE sqlite3_value *sqlite3ValueNew(sqlite3 *);
SQLITE_PRIVATE char *sqlite3Utf16to8(sqlite3 *, const void*, int, u8);
#ifdef SQLITE_ENABLE_STAT2
SQLITE_PRIVATE char *sqlite3Utf8to16(sqlite3 *, u8, char *, int, int *);
#endif
SQLITE_PRIVATE int sqlite3ValueFromExpr(sqlite3 *, Expr *, u8, u8, sqlite3_value **);
SQLITE_PRIVATE void sqlite3ValueApplyAffinity(sqlite3_value *, u8, u8);
#ifndef SQLITE_AMALGAMATION
SQLITE_PRIVATE const unsigned char sqlite3OpcodeProperty[];
SQLITE_PRIVATE const unsigned char sqlite3UpperToLower[];
SQLITE_PRIVATE const unsigned char sqlite3CtypeMap[];
SQLITE_PRIVATE const Token sqlite3IntTokens[];
SQLITE_PRIVATE SQLITE_WSD struct Sqlite3Config sqlite3Config;
SQLITE_PRIVATE SQLITE_WSD FuncDefHash sqlite3GlobalFunctions;
#ifndef SQLITE_OMIT_WSD
SQLITE_PRIVATE int sqlite3PendingByte;
#endif
#endif
SQLITE_PRIVATE void sqlite3RootPageMoved(sqlite3*, int, int, int);
SQLITE_PRIVATE void sqlite3Reindex(Parse*, Token*, Token*);
SQLITE_PRIVATE void sqlite3AlterFunctions(void);
SQLITE_PRIVATE void sqlite3AlterRenameTable(Parse*, SrcList*, Token*);
SQLITE_PRIVATE int sqlite3GetToken(const unsigned char *, int *);
SQLITE_PRIVATE void sqlite3NestedParse(Parse*, const char*, ...);
SQLITE_PRIVATE void sqlite3ExpirePreparedStatements(sqlite3*);
SQLITE_PRIVATE int sqlite3CodeSubselect(Parse *, Expr *, int, int);
SQLITE_PRIVATE void sqlite3SelectPrep(Parse*, Select*, NameContext*);
SQLITE_PRIVATE int sqlite3ResolveExprNames(NameContext*, Expr*);
SQLITE_PRIVATE void sqlite3ResolveSelectNames(Parse*, Select*, NameContext*);
SQLITE_PRIVATE int sqlite3ResolveOrderGroupBy(Parse*, Select*, ExprList*, const char*);
SQLITE_PRIVATE void sqlite3ColumnDefault(Vdbe *, Table *, int, int);
SQLITE_PRIVATE void sqlite3AlterFinishAddColumn(Parse *, Token *);
SQLITE_PRIVATE void sqlite3AlterBeginAddColumn(Parse *, SrcList *);
SQLITE_PRIVATE CollSeq *sqlite3GetCollSeq(sqlite3*, u8, CollSeq *, const char*);
SQLITE_PRIVATE char sqlite3AffinityType(const char*);
SQLITE_PRIVATE void sqlite3Analyze(Parse*, Token*, Token*);
SQLITE_PRIVATE int sqlite3InvokeBusyHandler(BusyHandler*);
SQLITE_PRIVATE int sqlite3FindDb(sqlite3*, Token*);
SQLITE_PRIVATE int sqlite3FindDbName(sqlite3 *, const char *);
SQLITE_PRIVATE int sqlite3AnalysisLoad(sqlite3*,int iDB);
SQLITE_PRIVATE void sqlite3DeleteIndexSamples(sqlite3*,Index*);
SQLITE_PRIVATE void sqlite3DefaultRowEst(Index*);
SQLITE_PRIVATE void sqlite3RegisterLikeFunctions(sqlite3*, int);
SQLITE_PRIVATE int sqlite3IsLikeFunction(sqlite3*,Expr*,int*,char*);
SQLITE_PRIVATE void sqlite3MinimumFileFormat(Parse*, int, int);
SQLITE_PRIVATE void sqlite3SchemaClear(void *);
SQLITE_PRIVATE Schema *sqlite3SchemaGet(sqlite3 *, Btree *);
SQLITE_PRIVATE int sqlite3SchemaToIndex(sqlite3 *db, Schema *);
SQLITE_PRIVATE KeyInfo *sqlite3IndexKeyinfo(Parse *, Index *);
SQLITE_PRIVATE int sqlite3CreateFunc(sqlite3 *, const char *, int, int, void *, 
  void (*)(sqlite3_context*,int,sqlite3_value **),
  void (*)(sqlite3_context*,int,sqlite3_value **), void (*)(sqlite3_context*),
  FuncDestructor *pDestructor
);
SQLITE_PRIVATE int sqlite3ApiExit(sqlite3 *db, int);
SQLITE_PRIVATE int sqlite3OpenTempDatabase(Parse *);

SQLITE_PRIVATE void sqlite3StrAccumInit(StrAccum*, char*, int, int);
SQLITE_PRIVATE void sqlite3StrAccumAppend(StrAccum*,const char*,int);
SQLITE_PRIVATE char *sqlite3StrAccumFinish(StrAccum*);
SQLITE_PRIVATE void sqlite3StrAccumReset(StrAccum*);
SQLITE_PRIVATE void sqlite3SelectDestInit(SelectDest*,int,int);
SQLITE_PRIVATE Expr *sqlite3CreateColumnExpr(sqlite3 *, SrcList *, int, int);

SQLITE_PRIVATE void sqlite3BackupRestart(sqlite3_backup *);
SQLITE_PRIVATE void sqlite3BackupUpdate(sqlite3_backup *, Pgno, const u8 *);

/*
** The interface to the LEMON-generated parser
*/
SQLITE_PRIVATE void *sqlite3ParserAlloc(void*(*)(size_t));
SQLITE_PRIVATE void sqlite3ParserFree(void*, void(*)(void*));
SQLITE_PRIVATE void sqlite3Parser(void*, int, Token, Parse*);
#ifdef YYTRACKMAXSTACKDEPTH
SQLITE_PRIVATE   int sqlite3ParserStackPeak(void*);
#endif

SQLITE_PRIVATE void sqlite3AutoLoadExtensions(sqlite3*);
#ifndef SQLITE_OMIT_LOAD_EXTENSION
SQLITE_PRIVATE   void sqlite3CloseExtensions(sqlite3*);
#else
# define sqlite3CloseExtensions(X)
#endif

#ifndef SQLITE_OMIT_SHARED_CACHE
SQLITE_PRIVATE   void sqlite3TableLock(Parse *, int, i64, u8, const char *);
#else
  #define sqlite3TableLock(v,w,x,y,z)
#endif

#ifdef SQLITE_TEST
SQLITE_PRIVATE   int sqlite3Utf8To8(unsigned char*);
#endif

#ifdef SQLITE_OMIT_VIRTUALTABLE
#  define sqlite3VtabClear(Y)
#  define sqlite3VtabSync(X,Y) SQLITE_OK
#  define sqlite3VtabRollback(X)
#  define sqlite3VtabCommit(X)
#  define sqlite3VtabInSync(db) 0
#  define sqlite3VtabLock(X) 
#  define sqlite3VtabUnlock(X)
#  define sqlite3VtabUnlockList(X)
#else
SQLITE_PRIVATE    void sqlite3VtabClear(sqlite3 *db, Table*);
SQLITE_PRIVATE    int sqlite3VtabSync(sqlite3 *db, char **);
SQLITE_PRIVATE    int sqlite3VtabRollback(sqlite3 *db);
SQLITE_PRIVATE    int sqlite3VtabCommit(sqlite3 *db);
SQLITE_PRIVATE    void sqlite3VtabLock(VTable *);
SQLITE_PRIVATE    void sqlite3VtabUnlock(VTable *);
SQLITE_PRIVATE    void sqlite3VtabUnlockList(sqlite3*);
#  define sqlite3VtabInSync(db) ((db)->nVTrans>0 && (db)->aVTrans==0)
#endif
SQLITE_PRIVATE void sqlite3VtabMakeWritable(Parse*,Table*);
SQLITE_PRIVATE void sqlite3VtabBeginParse(Parse*, Token*, Token*, Token*);
SQLITE_PRIVATE void sqlite3VtabFinishParse(Parse*, Token*);
SQLITE_PRIVATE void sqlite3VtabArgInit(Parse*);
SQLITE_PRIVATE void sqlite3VtabArgExtend(Parse*, Token*);
SQLITE_PRIVATE int sqlite3VtabCallCreate(sqlite3*, int, const char *, char **);
SQLITE_PRIVATE int sqlite3VtabCallConnect(Parse*, Table*);
SQLITE_PRIVATE int sqlite3VtabCallDestroy(sqlite3*, int, const char *);
SQLITE_PRIVATE int sqlite3VtabBegin(sqlite3 *, VTable *);
SQLITE_PRIVATE FuncDef *sqlite3VtabOverloadFunction(sqlite3 *,FuncDef*, int nArg, Expr*);
SQLITE_PRIVATE void sqlite3InvalidFunction(sqlite3_context*,int,sqlite3_value**);
SQLITE_PRIVATE int sqlite3VdbeParameterIndex(Vdbe*, const char*, int);
SQLITE_PRIVATE int sqlite3TransferBindings(sqlite3_stmt *, sqlite3_stmt *);
SQLITE_PRIVATE int sqlite3Reprepare(Vdbe*);
SQLITE_PRIVATE void sqlite3ExprListCheckLength(Parse*, ExprList*, const char*);
SQLITE_PRIVATE CollSeq *sqlite3BinaryCompareCollSeq(Parse *, Expr *, Expr *);
SQLITE_PRIVATE int sqlite3TempInMemory(const sqlite3*);
SQLITE_PRIVATE VTable *sqlite3GetVTable(sqlite3*, Table*);
SQLITE_PRIVATE const char *sqlite3JournalModename(int);
SQLITE_PRIVATE int sqlite3Checkpoint(sqlite3*, int, int, int*, int*);
SQLITE_PRIVATE int sqlite3WalDefaultHook(void*,sqlite3*,const char*,int);

/* Declarations for functions in fkey.c. All of these are replaced by
** no-op macros if OMIT_FOREIGN_KEY is defined. In this case no foreign
** key functionality is available. If OMIT_TRIGGER is defined but
** OMIT_FOREIGN_KEY is not, only some of the functions are no-oped. In
** this case foreign keys are parsed, but no other functionality is 
** provided (enforcement of FK constraints requires the triggers sub-system).
*/
#if !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)
SQLITE_PRIVATE   void sqlite3FkCheck(Parse*, Table*, int, int);
SQLITE_PRIVATE   void sqlite3FkDropTable(Parse*, SrcList *, Table*);
SQLITE_PRIVATE   void sqlite3FkActions(Parse*, Table*, ExprList*, int);
SQLITE_PRIVATE   int sqlite3FkRequired(Parse*, Table*, int*, int);
SQLITE_PRIVATE   u32 sqlite3FkOldmask(Parse*, Table*);
SQLITE_PRIVATE   FKey *sqlite3FkReferences(Table *);
#else
  #define sqlite3FkActions(a,b,c,d)
  #define sqlite3FkCheck(a,b,c,d)
  #define sqlite3FkDropTable(a,b,c)
  #define sqlite3FkOldmask(a,b)      0
  #define sqlite3FkRequired(a,b,c,d) 0
#endif
#ifndef SQLITE_OMIT_FOREIGN_KEY
SQLITE_PRIVATE   void sqlite3FkDelete(sqlite3 *, Table*);
#else
  #define sqlite3FkDelete(a,b)
#endif


/*
** Available fault injectors.  Should be numbered beginning with 0.
*/
#define SQLITE_FAULTINJECTOR_MALLOC     0
#define SQLITE_FAULTINJECTOR_COUNT      1

/*
** The interface to the code in fault.c used for identifying "benign"
** malloc failures. This is only present if SQLITE_OMIT_BUILTIN_TEST
** is not defined.
*/
#ifndef SQLITE_OMIT_BUILTIN_TEST
SQLITE_PRIVATE   void sqlite3BeginBenignMalloc(void);
SQLITE_PRIVATE   void sqlite3EndBenignMalloc(void);
#else
  #define sqlite3BeginBenignMalloc()
  #define sqlite3EndBenignMalloc()
#endif

#define IN_INDEX_ROWID           1
#define IN_INDEX_EPH             2
#define IN_INDEX_INDEX           3
SQLITE_PRIVATE int sqlite3FindInIndex(Parse *, Expr *, int*);

#ifdef SQLITE_ENABLE_ATOMIC_WRITE
SQLITE_PRIVATE   int sqlite3JournalOpen(sqlite3_vfs *, const char *, sqlite3_file *, int, int);
SQLITE_PRIVATE   int sqlite3JournalSize(sqlite3_vfs *);
SQLITE_PRIVATE   int sqlite3JournalCreate(sqlite3_file *);
#else
  #define sqlite3JournalSize(pVfs) ((pVfs)->szOsFile)
#endif

SQLITE_PRIVATE void sqlite3MemJournalOpen(sqlite3_file *);
SQLITE_PRIVATE int sqlite3MemJournalSize(void);
SQLITE_PRIVATE int sqlite3IsMemJournal(sqlite3_file *);

#if SQLITE_MAX_EXPR_DEPTH>0
SQLITE_PRIVATE   void sqlite3ExprSetHeight(Parse *pParse, Expr *p);
SQLITE_PRIVATE   int sqlite3SelectExprHeight(Select *);
SQLITE_PRIVATE   int sqlite3ExprCheckHeight(Parse*, int);
#else
  #define sqlite3ExprSetHeight(x,y)
  #define sqlite3SelectExprHeight(x) 0
  #define sqlite3ExprCheckHeight(x,y)
#endif

SQLITE_PRIVATE u32 sqlite3Get4byte(const u8*);
SQLITE_PRIVATE void sqlite3Put4byte(u8*, u32);

#ifdef SQLITE_ENABLE_UNLOCK_NOTIFY
SQLITE_PRIVATE   void sqlite3ConnectionBlocked(sqlite3 *, sqlite3 *);
SQLITE_PRIVATE   void sqlite3ConnectionUnlocked(sqlite3 *db);
SQLITE_PRIVATE   void sqlite3ConnectionClosed(sqlite3 *db);
#else
  #define sqlite3ConnectionBlocked(x,y)
  #define sqlite3ConnectionUnlocked(x)
  #define sqlite3ConnectionClosed(x)
#endif

#if defined(SQLITE_DEBUG)
SQLITE_PRIVATE   void sqlite3ParserTrace(FILE*, char *);
#endif

/*
** If the SQLITE_ENABLE IOTRACE exists then the global variable
** sqlite3IoTrace is a pointer to a printf-like routine used to
** print I/O tracing messages. 
*/
#ifdef SQLITE_ENABLE_IOTRACE
# define IOTRACE(A)  if( sqlite3IoTrace ){ sqlite3IoTrace A; }
SQLITE_PRIVATE   void sqlite3VdbeIOTraceSql(Vdbe*);
SQLITE_PRIVATE void (*sqlite3IoTrace)(const char*,...);
#else
# define IOTRACE(A)
# define sqlite3VdbeIOTraceSql(X)
#endif

/*
** These routines are available for the mem2.c debugging memory allocator
** only.  They are used to verify that different "types" of memory
** allocations are properly tracked by the system.
**
** sqlite3MemdebugSetType() sets the "type" of an allocation to one of
** the MEMTYPE_* macros defined below.  The type must be a bitmask with
** a single bit set.
**
** sqlite3MemdebugHasType() returns true if any of the bits in its second
** argument match the type set by the previous sqlite3MemdebugSetType().
** sqlite3MemdebugHasType() is intended for use inside assert() statements.
**
** sqlite3MemdebugNoType() returns true if none of the bits in its second
** argument match the type set by the previous sqlite3MemdebugSetType().
**
** Perhaps the most important point is the difference between MEMTYPE_HEAP
** and MEMTYPE_LOOKASIDE.  If an allocation is MEMTYPE_LOOKASIDE, that means
** it might have been allocated by lookaside, except the allocation was
** too large or lookaside was already full.  It is important to verify
** that allocations that might have been satisfied by lookaside are not
** passed back to non-lookaside free() routines.  Asserts such as the
** example above are placed on the non-lookaside free() routines to verify
** this constraint. 
**
** All of this is no-op for a production build.  It only comes into
** play when the SQLITE_MEMDEBUG compile-time option is used.
*/
#ifdef SQLITE_MEMDEBUG
SQLITE_PRIVATE   void sqlite3MemdebugSetType(void*,u8);
SQLITE_PRIVATE   int sqlite3MemdebugHasType(void*,u8);
SQLITE_PRIVATE   int sqlite3MemdebugNoType(void*,u8);
#else
# define sqlite3MemdebugSetType(X,Y)  /* no-op */
# define sqlite3MemdebugHasType(X,Y)  1
# define sqlite3MemdebugNoType(X,Y)   1
#endif
#define MEMTYPE_HEAP       0x01  /* General heap allocations */
#define MEMTYPE_LOOKASIDE  0x02  /* Might have been lookaside memory */
#define MEMTYPE_SCRATCH    0x04  /* Scratch allocations */
#define MEMTYPE_PCACHE     0x08  /* Page cache allocations */
#define MEMTYPE_DB         0x10  /* Uses sqlite3DbMalloc, not sqlite_malloc */

#ifdef __cplusplus
};  /* end of the 'extern "C"' block */
#endif

#endif /* _SQLITEINT_H_ */
/************** End of sqliteInt.h *******************************************/
