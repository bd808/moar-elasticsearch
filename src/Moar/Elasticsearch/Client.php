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

  /**
   * Default server URL.
   * @var string
   */
  const DEFAULT_SERVER = 'http://localhost:9200';

  /**
   * Server URL.
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
   * User for HTTP basic auth.
   * @var string
   */
  protected $user;

  /**
   * Password for HTTP auth.
   * @var string
   */
  protected $password;

  /**
   * cURL auth type.
   * @var int
   */
  protected $authType = CURLAUTH_BASIC;

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

    $conn = new self();
    return $conn->setFromUrl($url);
  } //end connection

  /**
   * Set server, index, type, user and password from a URL.
   *
   * @param string $url Server URL
   * @return Client Self, for message chaining
   */
  public function setFromUrl ($url) {
    $parts = parse_url($url);
    if (isset($parts['host'])) {
      $server = (isset($parts['scheme']))? $parts['scheme']: 'http';
      $server .= "://{$parts['host']}";
      if (isset($parts['port'])) {
        $server .= ":{$parts['port']}";
      }
      $this->setServer($server);
    }

    if (isset($parts['path']) && !empty($parts['path'])) {
      // use array_filter() to discard empty parts
      $path = array_filter(explode('/', $parts['path']));
      if (!empty($path)) {
        $this->setIndex(array_shift($path));
      }
      if (!empty($path)) {
        $this->setType(array_shift($path));
      }
    }

    if (isset($parts['user'])) {
      $this->setUser($parts['user']);
    }
    if (isset($parts['pass'])) {
      $this->setPassword($parts['pass']);
    }

    return $this;
  } //end setFromUrl

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

  /**
   * Get the currently configured server.
   *
   * @return string Server
   */
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

  /**
   * Get the currently configured index.
   *
   * @return string Index
   */
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

  /**
   * Get the currently configured type.
   *
   * @return string Type
   */
  public function type () {
    return $this->type;
  }

  /**
   * Set user for HTTP auth.
   *
   * @param string $u User
   * @return Client Self, for message chaining
   */
  public function setUser ($u) {
    $this->user = $u;
    return $this;
  }

  /**
   * Get the currently configured HTTP user.
   *
   * @return string User
   */
  public function user () {
    return $this->user;
  }

  /**
   * Set password for HTTP auth.
   *
   * @param string $p Password
   * @return Client Self, for message chaining
   */
  public function setPassword ($p) {
    $this->password = $p;
    return $this;
  }

  /**
   * Get the currently configured HTTP password.
   *
   * @return string Password
   */
  public function password () {
    return $this->password;
  }

  /**
   * Set the HTTP authentication type.
   *
   * @param int $t Auth type
   * @return Client Self, for message chaining
   * @see \curl_setopt()
   * @see CURLOPT_HTTPAUTH
   */
  public function setAuthType ($t) {
    $this->authType = $t;
    return $this;
  }

  /**
   * Get the currently configured HTTP authentication type.
   *
   * @return int HTTP authentication type
   */
  public function authType () {
    return $this->authType;
  }

  /**
   * Execute a search.
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
    $this->addCredentials($req);
    $resp = $req->submit(false);
    return new Response(
        $resp->getResponseBody(), $resp->getResponseHttpCode());
  } //end search

  /**
   * Execute a scan query.
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
    $this->addCredentials($req);
    $resp = $req->submit(false);
    return new Response(
        $resp->getResponseBody(), $resp->getResponseHttpCode(),
        $this->server, $keepAlive);
  } //end scan

  /**
   * Submit a bulk instruction set to cluster.
   *
   * Each "operation" in the array is expected to be:
   * - an "action_and_meta_data" json string
   * - an "optional_source" json string
   * - an "action_and_meta_data"\n"optional_source" pair
   *
   * Array input will be flattened into a string by `implode("\n", ...)`.
   *
   * @param array|string $data Bulk operations list
   * @param array  $opts Curl configuration options
   * @return Response ElasticSearch response
   * @see http://www.elasticsearch.org/guide/reference/api/bulk/
   */
  public function bulk ($data, $opts = null) {
    if (is_array($data)) {
      $payload = implode("\n", $data) . "\n";
    } else {
      $payload = (string) $data;
    }

    $req = new HttpRequest($this->buildUrl('_bulk'), 'PUT');
    $req->setHeaders(array('Content-type: application/json'));
    $req->setPostBody($payload);
    $req->setCurlOptions($opts);
    $this->addCredentials($req);
    $resp = $req->submit(false);
    return new Response(
        $resp->getResponseBody(), $resp->getResponseHttpCode());
  } //end bulk

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

  /**
   * Add authentication credentials to a Moar\Net\Http\Request.
   *
   * @param Moar\Net\Http\Request $req Request to alter
   * @return void
   */
  protected function addCredentials ($req) {
    if (null !== $this->user) {
      $req->setCredentials($this->user, $this->password, $this->authType);
    }
  } //end addCredentials

} //end Client
