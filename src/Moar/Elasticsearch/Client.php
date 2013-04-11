<?php
/**
 * @package Moar\Elasticsearch
 */

namespace Moar\Elasticsearch;

use Moar\Net\Http\Request as HttpRequest;

/**
 * ElasticSearch client.
 *
 * @package Moar\Elasticsearch
 * @copyright 2013 Bryan Davis and contributors. All Rights Reserved.
 */
class Client {

  const DEFAULT_SERVER = 'http://localhost:9200';

  /**
   * Server to contact.
   * @var string
   */
  protected $server = self::DEFAULT_SERVER;

  /**
   * Index to query.
   * @var string
   */
  protected $index;

  /**
   * Document type to query.
   * @var string
   */
  protected $type;

  /**
   * Constructor.
   *
   * @param string $svr Server URL (including scheme and port)
   * @param string|array $idx Index name(s)
   * @param string|array $type Document type
   */
  public function __construct ($svr = null, $idx = null, $type = null) {
    if (null !== $svr) {
      $this->setServer($svr);
    }
    if (null !== $idx) {
      $this->setIndex($idx);
    }
    if (null !== $type) {
      $this->setType($type);
    }
  }

  /**
   * Connection factory.
   *
   * If no URL is provided the environment variable `ELASTICSEARCH_URL` will
   * be used.
   *
   * @param string $url Server URL
   * @return Client New client
   */
  public static function connection ($url = null) {
    if (null === $url) {
      $url = getenv('ELASTICSEARCH_URL');
    }

    $server = null;
    $index = null;
    $type = null;

    $parts = parse_url($url);
    if (isset($parts['host'])) {
      $server = (isset($parts['scheme']))? $parts['scheme']: 'http';
      $server .= "://{$parts['host']}";
      if (isset($parts['port'])) {
        $server .= ":{$parts['port']}";
      }
    }

    if (isset($parts['path']) && !empty($parts['path'])) {
      $path = array_filter(explode('/', $parts['path']));
      $index = (empty($path))? null: array_shift($path);
      $type = (empty($path))? null: array_shift($path);
    }

    return new self($server, $index, $type);
  } //end connection

  /**
   * Set server to query.
   *
   * @param string $url Server URL (including scheme and port)
   * @return Client Self, for message chaining
   */
  public function setServer ($url) {
    $this->server = $url;
    return $this;
  }

  public function server () {
    return $this->server;
  }

  /**
   * Set index to query.
   *
   * @param string $idx Index name(s)
   * @return Client Self, for message chaining
   */
  public function setIndex ($idx) {
    if (is_array($idx)) {
      $idx = implode(',', array_filter($idx));
    }
    $this->index = $idx;
    return $this;
  }

  public function index () {
    return $this->index;
  }

  /**
   * Set document type to query.
   *
   * @param string $type Document type
   * @return Client Self, for message chaining
   */
  public function setType ($type) {
    if (is_array($type)) {
      $type = implode(',', array_filter($type));
    }
    $this->type = $type;
    return $this;
  }

  public function type () {
    return $this->type;
  }

  /**
   * Execute query.
   *
   * @param string|object $json Json query
   * @param array  $opts Curl configuration options
   * @return Response ElasticSearch response
   */
  public function search ($json, $opts = null) {
    if (!is_string($json)) {
      $json = json_encode($json);
    }

    $req = new HttpRequest($this->buildUrl('_search'), 'GET');
    $req->setHeaders(array('Content-type: application/json'));
    $req->setPostBody($json);
    $req->setCurlOptions($opts);
    $resp = $req->submit(false);
    return new Response(
        $resp->getResponseBody(), $resp->getResponseHttpCode());
  }

  /**
   * Execute scan query.
   *
   * @param string|object $json Json query
   * @param int $fetch Number of records to fetch per request
   * @param string $keepAlive Duration to keep cursor alive between requests
   * @param array  $opts Curl configuration options
   * @return Response ElasticSearch response
   */
  public function scan (
      $json, $fetch = 50, $keepAlive = '1m', $opts = null) {
    if (!is_string($json)) {
      $json = json_encode($json);
    }
    $req = new HttpRequest($this->buildUrl('_search'), 'GET');
    $req->addQueryData(array(
        'search_type' => 'scan',
        'scroll' => $keepAlive,
        'size' => $fetch,
      ));
    $req->setHeaders(array('Content-type: application/json'));
    $req->setPostBody($json);
    $req->setCurlOptions($opts);
    $resp = $req->submit(false);
    return new Response(
        $resp->getResponseBody(), $resp->getResponseHttpCode(),
        $this->server, $keepAlive);
  } //end scan

  /**
   * Build the URL for a given action.
   *
   * @param string $action ElasticSearch action
   */
  protected function buildUrl ($action) {
    $parts = array();
    $parts[] = $this->server;
    if (isset($this->index)) {
      $parts[] = urlencode($this->index);

      if (isset($this->type)) {
        $parts[] = urlencode($this->type);
      }
    }
    $parts[] = urlencode($action);
    return implode('/', $parts);
  } //end buildUrl

} //end Client
