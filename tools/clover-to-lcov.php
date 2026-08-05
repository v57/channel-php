#!/usr/bin/env php
<?php
declare(strict_types=1);

if ($argc < 3) {
    fwrite(STDERR, "Usage: php tools/clover-to-lcov.php <clover.xml> <lcov.info>\n");
    exit(1);
}

$input = $argv[1];
$output = $argv[2];

if (!is_file($input)) {
    fwrite(STDERR, "Clover file not found: {$input}\n");
    exit(1);
}

libxml_use_internal_errors(true);
$xml = simplexml_load_file($input);
if (!$xml instanceof SimpleXMLElement) {
    fwrite(STDERR, "Failed to parse Clover XML: {$input}\n");
    exit(1);
}

$files = $xml->xpath('//file');
if (!is_array($files)) {
    fwrite(STDERR, "No <file> entries found in Clover XML.\n");
    exit(1);
}

$lines = [];

foreach ($files as $file) {
    if (!$file instanceof SimpleXMLElement) {
        continue;
    }

    $name = trim((string) ($file['name'] ?? ''));
    if ($name === '') {
        continue;
    }

    $coverage = [];
    foreach ($file->line as $line) {
        $type = (string) ($line['type'] ?? '');
        if ($type !== 'stmt') {
            continue;
        }

        $lineNo = (int) ($line['num'] ?? 0);
        if ($lineNo <= 0) {
            continue;
        }

        $count = (int) ($line['count'] ?? 0);
        if (isset($coverage[$lineNo])) {
            $coverage[$lineNo] = max($coverage[$lineNo], $count);
        } else {
            $coverage[$lineNo] = $count;
        }
    }

    if ($coverage === []) {
        continue;
    }

    ksort($coverage, SORT_NUMERIC);
    $lines[] = [
        'file' => $name,
        'coverage' => $coverage,
    ];
}

if ($lines === []) {
    fwrite(STDERR, "No executable statement coverage found in Clover XML.\n");
    exit(1);
}

$dir = dirname($output);
if (!is_dir($dir)) {
    if (!mkdir($dir, 0777, true) && !is_dir($dir)) {
        fwrite(STDERR, "Failed to create output directory: {$dir}\n");
        exit(1);
    }
}

$fp = fopen($output, 'wb');
if ($fp === false) {
    fwrite(STDERR, "Failed to open output file: {$output}\n");
    exit(1);
}

foreach ($lines as $entry) {
    fwrite($fp, "TN:\n");
    fwrite($fp, 'SF:' . $entry['file'] . "\n");

    $lf = count($entry['coverage']);
    $lh = 0;

    foreach ($entry['coverage'] as $lineNo => $count) {
        if ($count > 0) {
            $lh++;
        }
        fwrite($fp, 'DA:' . $lineNo . ',' . $count . "\n");
    }

    fwrite($fp, 'LF:' . $lf . "\n");
    fwrite($fp, 'LH:' . $lh . "\n");
    fwrite($fp, "end_of_record\n");
}

fclose($fp);
exit(0);
