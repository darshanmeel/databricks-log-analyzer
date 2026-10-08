// What kind of failure an error is, in plain words, from its exception class and message. One list for every page,
// so the same error gets the same name (and the same rank) on the run, the cluster and the Errors page.
import type { ErrorGroup } from './api';

export interface ErrorClass {
  key: string;
  label: string;
  /** 2: likely made something fail; 1: worth a look; 0: a warning that changes nothing */
  effect: 0 | 1 | 2;
  hint: string;
}

const CLASSES: [RegExp, ErrorClass][] = [
  [/instead|deprecat|not supported in this version/i,
    { key: 'warning', label: 'warning', effect: 0, hint: 'A notice about an option or API; nothing failed because of it.' }],
  [/SAS token|AuthorizationPermissionMismatch|AuthenticationFailed|AccessDenied|access denied|forbidden|unauthori[sz]ed|\b403\b|\b401\b|PERMISSION_DENIED|credential|InvalidAuthenticationInfo|token.*expired/i,
    { key: 'access', label: 'credentials or access', effect: 2, hint: 'Storage or a service refused the request: an expired token or SAS, a missing grant, or the wrong identity.' }],
  [/FAILED_READ_FILE|FileNotFound|No such file|PathNotFound|does not exist|DELTA_FILE_NOT_FOUND|FileReadException/i,
    { key: 'read', label: 'missing or unreadable file', effect: 2, hint: 'A file was gone or could not be read: a VACUUM, an overwrite during the read, or a broken path.' }],
  [/OutOfMemory|Java heap space|GC overhead|Container killed|exit code 137|MemoryError|SparkOutOfMemory/i,
    { key: 'memory', label: 'out of memory', effect: 2, hint: 'A JVM or Python worker ran out of memory: tasks too big, a large broadcast or collect, or too little memory per core.' }],
  [/No space left/i,
    { key: 'disk', label: 'disk full', effect: 2, hint: 'A worker ran out of local disk, usually from spill or shuffle files.' }],
  // before "network": a JDBC driver's timeout is the source database, not a lost executor
  [/(com\.sap\.db|oracle\.jdbc|sqlserver|postgresql|com\.mysql|mariadb|db2|snowflake|teradata|jdbc)[\s\S]*(timed out|SocketTimeout|Data receive failed|Connection reset|cannot open socket|Communications link failure)|(timed out|SocketTimeout|Data receive failed|Connection reset|Communications link failure)[\s\S]*(com\.sap\.db|oracle\.jdbc|sqlserver|postgresql|com\.mysql|mariadb|db2|snowflake|teradata|jdbc)/i,
    { key: 'source-db', label: 'source database connection', effect: 1, hint: 'The connection to the source database dropped or timed out. Spark retries the task; check the database, the network and the number of parallel JDBC connections.' }],
  [/FetchFailed|Lost executor|ExecutorLostFailure|Connection (refused|reset)|ConnectException|SocketTimeout|UnknownHost|Broken pipe/i,
    { key: 'network', label: 'network or lost executor', effect: 2, hint: 'Shuffle data or an executor could not be reached: a lost or decommissioned node, or the network.' }],
  [/Timeout|timed out/i,
    { key: 'timeout', label: 'timeout', effect: 2, hint: 'Something waited too long: a service, a lock, a broadcast or the job itself.' }],
  [/AnalysisException|UNRESOLVED_|cannot resolve|Column .* not found|TABLE_OR_VIEW_NOT_FOUND|SCHEMA|ParseException/i,
    { key: 'query', label: 'query or schema', effect: 2, hint: 'Spark could not run the query as written: a missing table or column, a schema mismatch, a syntax error.' }],
  [/NumberFormat|CAST_INVALID|Cast|ArithmeticException|DIVIDE_BY_ZERO|NullPointer|ArrayIndexOutOfBounds|Malformed/i,
    { key: 'data', label: 'bad data', effect: 2, hint: 'A value did not fit what the code expected: a failed cast, a null, a malformed record.' }],
  [/DeltaConcurrent|io\.delta\S*Concurrent|Concurrent(Append|DeleteRead|DeleteDelete|Transaction|Write)Exception|MetadataChangedException|ProtocolChangedException|DELTA_CONCURRENT/,
    { key: 'conflict', label: 'concurrent write', effect: 2, hint: 'Two writers changed the same Delta table at once; one lost.' }],
  [/PythonException|Py4JJavaError|Traceback/i,
    { key: 'python', label: 'Python error', effect: 2, hint: 'Raised in Python code (a UDF or the notebook); the stack shows the line.' }],
  [/Cancel|interrupted|InterruptedException|killed/i,
    { key: 'cancel', label: 'cancelled', effect: 1, hint: 'Work was stopped: a cancel, a timeout, or the cluster stopping.' }],
  [/FAILED_|Failed to|failure|Exception/i,
    { key: 'other', label: 'error', effect: 1, hint: '' }],
];

const FALLBACK: ErrorClass = { key: 'other', label: 'error', effect: 1, hint: '' };

export function errorClass(e: Pick<ErrorGroup, 'exception_class' | 'sample_message'>): ErrorClass {
  const t = `${e.exception_class ?? ''} ${e.sample_message ?? ''}`;
  return CLASSES.find(([re]) => re.test(t))?.[1] ?? FALLBACK;
}
