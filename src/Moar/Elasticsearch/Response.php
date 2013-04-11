<?php
/**
 * @package Moar\Elasticsearch
 */

namespace Moar\Elasticsearch;

use Moar\Net\Http\Request as HttpRequest;

/**
 * Elastic Search response wrapper.
 *
 * @package Moar\Elasticsearch
 * @copyright 2013 Bryan Davis and contributors. All Rights Reserved.
 */
class Response implements \Iterator, \Countable {

  /**
   * Was this an error response?
   * @var bool
   */
  protected $isError;

  /**
   * Convenience pointer to returned documents.
   * @var array
   */
  protected $results;

  /**
   * Iteration pointer.
   * @var int
   */
  protected $ptr;

  /**
   * Offset in scrolling result set.
   * @var int
   */
  protected $offset = 0;

  /**
   * Url for scroll requests.
   * @var string
   */
  protected $scrollUrl;

  /**
   * Keep alive time for scroll requests.
   * @var string
   */
  protected $keepAlive;

  /**
   * Constructor.
   *
   * @param string|object $body ES response body
   * @param int $status HTTP response status code
   * @param string $url ElasticSearch server URL for scroll requests
   * @param string $keepAlive Duration to keep cursor alive between requests
   */
  public function __construct (
      $body, $status = 200, $url = null, $keepAlive = null) {
    $this->processResponse($body, $status);

    if (isset($this->_scroll_id) && null !== $url) {
      $this->scrollUrl = "{$url}/_search/scroll";
      $this->keepAlive = $keepAlive;
    }
  } //end __construct

  /**
   * Process an HTTP response.
   *
   * @param string|object $body ES response body
   * @param int $status HTTP response status code
   */
  protected function processResponse ($body, $status) {
    unset($this->_scroll_id);

    // check for http error response
    $this->isError = ($status < 200 || $status > 299);

    if (is_string($body)) {
      // decode payload
      $body = json_decode($body);
    }

    if (empty($body)) {
      $body = new \stdClass();
      $this->isError = true;
    }

    // copy response properties onto ourself.
    // this allows us to json serialize and still look the same as the original
    // response.
    if (is_object($body) || is_array($body)) {
      foreach ($body as $prop => $value) {
        $this->{$prop} = $value;
      }
    }

    // pick out results from response (fake if necessary)
    $this->results = (isset($this->hits->hits))? $this->hits->hits: array();
    $this->rewind();
  } //end processResponse

  /**
   * Is this an error response?
   *
   * @return bool True if response encodes an error, false otherwise
   */
  public function isError () {
    return $this->isError;
  }

  /**
   * Get the collection of results from the request.
   * @return array Results
   */
  public function getResults () {
    return $this->results;
  }

  /**
   * Does this response have facet information?
   * @return bool True if facets are present, false otehrwise
   */
  public function hasFacets () {
    return isset($this->facets);
  }

  /**
   * Get facets.
   * @return array Facets
   */
  public function getFacets () {
    return ($this->hasFacets())? $this->facets: array();
  }

  /**
   * Get the total number of matches for the request.
   * @return int Total number of documents matched by request
   */
  public function getTotalHits () {
    return (isset($this->hits->total))? $this->hits->total: 0;
  }

  /**
   * Get the amount of time that elapsed to perform request according to
   * Elastic Search cluster.
   * @return int Elapsed time in milliseconds
   */
  public function getElapsed () {
    return (isset($this->took))? $this->took: 0;
  }

  /**
   * Get count of results.
   *
   * @return int Count of results in this response
   */
  public function count () {
    return count($this->results);
  }

  /**
   * Return the current iterator object.
   * @return object Result
   */
  public function current () {
    return $this->results[$this->ptr];
  }

  /**
   * Return the current iterator slot key.
   * @return int Key
   */
  public function key () {
    return $this->ptr + $this->offset;
  }

  /**
   * Advance the iterator to the next slot.
   * @return void
   */
  public function next () {
    $this->ptr++;
  }

  /**
   * Reset iterator to begenning of collection.
   * @return void
   */
  public function rewind () {
    $this->ptr = 0;
  }

  /**
   * Check to see if current iterator position is valid.
   * @return bool True if pointing to valid member, false otherwise
   */
  public function valid () {
    $val = isset($this->results[$this->ptr]);
    if (!$val && isset($this->_scroll_id)) {
      $this->scroll();
      $val = isset($this->results[$this->ptr]) && !$this->isError();
    }
    return $val;
  }

  /**
   * Scroll to the next chunk of response data.
   *
   * This will be called automatically under iteration (eg foreach) when the
   * current results are exhausted if _scroll_id is present.
   *
   * @return void
   */
  public function scroll () {
    if (!isset($this->_scroll_id)) {
      throw new \DomainException('Not a scrollable response.');
    }

    // keep track of our offset in the larger result set
    $this->offset = count($this->results) - 1;

    $req = new HttpRequest($this->scrollUrl, 'GET');
    $req->addQueryData(array(
        'scroll' => $this->keepAlive,
      ));
    $req->setPostBody($this->_scroll_id);
    $resp = $req->submit(false);
    $this->processResponse(
        $resp->getResponseBody(), $resp->getResponseHttpCode());
  } //end scroll

} //end Response
