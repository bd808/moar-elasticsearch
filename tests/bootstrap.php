<?php
require_once __DIR__ . '/../vendor/autoload.php';

// Twelve-Factor App configuration
// populate $_ENV from .env if present
if (is_readable(__DIR__ . '/../.env')) {
  $lines = explode("\n", file_get_contents(__DIR__ . '/../.env'));

  foreach ($lines as $line) {
    // variable extraction logic lifted from
    // https://github.com/bkeepers/dotenv
    if (preg_match('/\A(?:export\s+)?(\w+)(?:=|: ?)(.*)\z/', $line, $group)) {
      $key = $group[1];
      $val = $group[2];
      if (preg_match('/\A\'(.*)\'\z/', $val, $group)) {
        $val = $group[1];
      } else if (preg_match('/\A"(.*)"\z/', $val, $group)) {
        $val = $group[1];
      }
      // store in super global
      $_ENV[$key] = $val;
      // also store in process env vars
      putenv("{$key}={$val}");
    }
  } //end foreach line
} //end if .env
