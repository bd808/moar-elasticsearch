<?php
/**
 * @package Moar\Elasticsearch
 */

namespace Moar\Elasticsearch;

use Moar\Net\Http\Request as HttpRequest;

/**
 * Fluent ElasticSearch query builder.
 *
 * The json format used by Elastic Search for requests and php's default
 * stdClass object behavior work very nicely together for creation of queries,
 * but there are some boring boilerplate parts that this class can help out
 * with.
 *
 * This class (ab)uses the `__get` and `__set` magic functions and
 * \ArrayAccess::offsetSet() to provide \stdClass like automatic member
 * creation functionality. A call to get an non-existant member will add a new
 * Query instance in that member slot and return it via `__get`. `__set`
 * detects addition of Query members and provides them with information about
 * the parent object and member name where they have been stored.
 * `offsetSet()` uses the stored parent and member name data to replace the
 * current object with a native PHP array in the parent object. When used in
 * concert, these methods allow chained syntax for manipulating deeply nested
 * object structures common in ElasticSearch query language while
 * simultaneously providing convenience functions at any depth.
 *
 * @package Moar\Elasticsearch
 * @copyright 2013 Bryan Davis and contributors. All Rights Reserved.
 */
class Query implements \ArrayAccess {

  /**
   * Sort order: ascending.
   * @var string
   */
  const SORT_ASC = 'asc';

  /**
   * Sort order: ascending.
   * @var string
   */
  const SORT_DESC = 'desc';

  /**
   * Pointer to parent object.
   * @var object
   */
  protected $_parent;

  /**
   * Property name we are stored under in parent.
   * @var string
   */
  protected $_slot;

  /**
   * Server to contact.
   * @var string
   */
  protected $_server;

  /**
   * Index to query.
   * @var string
   */
  protected $_index;

  /**
   * Document type to query.
   * @var string
   */
  protected $_type;

  /**
   * Constructor.
   *
   * @param string $svr Server URL (including scheme and port)
   * @param string $idx Index name(s)
   * @param string $type Document type
   * @param array $props Initial properties
   */
  public function __construct (
      $svr = null, $idx = null, $type = null, $props = array()) {
    if (null !== $svr) {
      $this->_server = $svr;
    }
    if (null !== $idx) {
      $this->_index = $idx;
    }
    if (null !== $type) {
      $this->_type = $type;
    }

    foreach ($props as $slot => $value) {
      $this->{$slot} = $value;
    }
  }

  /**
   * Set server to query.
   *
   * @param string $url Server URL (including scheme and port)
   * @return Query Self, for message chaining
   */
  public function server ($url) {
    $this->_server = $url;
    return $this;
  }

  /**
   * Set index to query.
   *
   * @param string $idx Index name(s)
   * @return Query Self, for message chaining
   */
  public function index ($idx) {
    $this->_index = $idx;
    return $this;
  }

  /**
   * Set document type to query.
   *
   * @param string $type Document type
   * @return Query Self, for message chaining
   */
  public function type ($type) {
    $this->_type = $type;
    return $this;
  }

  /**
   * Instance factory.
   *
   * @param array $props Initial properties
   * @return Query New instance
   */
  public static function getInstance ($props = array()) {
    return new Query(null, null, null, $props);
  } //end getInstance

  /**
   * Create a node with non-empty search parameters AND'd
   * together as termFilters.
   *
   * @param array $termMap Term keys to search value
   * @return Query Node with ->and[] of non-empty search terms.
   */
  public static function andTerms ($termMap) {
    $ands = new Query();

    foreach ($termMap as $term => $value) {
      if (null !== $value && '' !== $value) {
        $ands->andTermFilter($term, $value);
      }
    }

    return $ands;
  } // end andTerms

  /**
   * Create a node with non-empty search parameters OR'd
   * together as termFilters.
   *
   * @param array $termMap Term keys to search value
   * @return Query Node with ->or[] of non-empty search terms.
   */
  public static function orTerms ($termMap) {
    $ors = new Query();

    foreach ($termMap as $term => $value) {
      if (null !== $value && '' !== $value) {
        $ors->orTermFilter($term, $value);
      }
    }

    return $ors;
  } // end orTerms

  /**
   * Does this node have a parent?
   * @return bool True if parent is set, false otherwise
   */
  public function hasParent () {
    return null !== $this->_parent;
  } //end hasParent

  /**
   * Do we have a property with the given name?
   *
   * @param string $name Property name
   * @return bool True if instance has property, false otherwise
   */
  public function has ($name) {
    return isset($this->{$name});
  } //end has

  /**
   * Convert graph rooted at this node to a json string.
   *
   * @return string Json encoding of graph
   */
  public function json () {
    return json_encode($this);
  }

  /**
   * Execute query.
   *
   * @param array  $opts Curl configuration options
   * @return Response ElasticSearch response
   */
  public function search ($opts = null) {
    $req = new HttpRequest($this->_buildUrl('_search'), 'GET');
    $req->setHeaders(array('Content-type: application/json'));
    $req->setPostBody($this->json());
    $req->setCurlOptions($opts);
    $resp = $req->submit(false);
    return new Response(
        $resp->getResponseBody(), $resp->getResponseHttpCode());
  }

  /**
   * Execute scan query.
   *
   * @param int $fetch Number of records to fetch per request
   * @param string $keepAlive Duration to keep cursor alive between requests
   * @param array  $opts Curl configuration options
   * @return Response ElasticSearch response
   */
  public function scan (
      $fetch = 50, $keepAlive = '1m', $opts = null) {
    $req = new HttpRequest($this->_buildUrl('_search'), 'GET');
    $req->addQueryData(array(
        'search_type' => 'scan',
        'scroll' => $keepAlive,
        'size' => $fetch,
      ));
    $req->setHeaders(array('Content-type: application/json'));
    $req->setPostBody($this->json());
    $req->setCurlOptions($opts);
    $resp = $req->submit(false);
    return new Response(
        $resp->getResponseBody(), $resp->getResponseHttpCode(),
        $this->_server, $keepAlive);
  } //end scan

  /**
   * Build the URL for a given action.
   *
   * @param string $action ElasticSearch action
   */
  protected function _buildUrl ($action) {
    $parts = array();
    $parts[] = $this->_server;
    if (isset($this->_index)) {
      $parts[] = urlencode($this->_index);

      if (isset($this->_type)) {
        $parts[] = urlencode($this->_type);
      }
    }
    $parts[] = urlencode($action);
    return implode('/', $parts);
  } //end _buildUrl

  /**
   * Add sort criteria to this node.
   *
   * @param string $field Field to sort on
   * @param string $order Sort order
   * @return Query Self, for message chaining
   */
  public function sort ($field, $order = self::SORT_ASC) {
    $clause = new Query();
    $clause->{$field}->order = $order;
    $this->sort[] = $clause;
    return $this;
  } //end sort

  /**
   * Add a script sort criteria to this node.
   *
   * @param string $script Script source or stored script name
   * @param string $type Data type returned by script
   * @param array $params Script parameters
   * @param string $order Sort order
   * @return Query Self, for message chaining
   */
  public function scriptSort (
      $script, $type, $params = array(), $order = self::SORT_ASC) {
    $clause = new Query();
    $clause->_script->script = $script;
    $clause->_script->type = $type;
    if (!empty($params)) {
      $clause->_script->params = $params;
    }
    $clause->_script->order = $order;
    $this->sort[] = $clause;
    return $this;
  } //end scriptSort

  /**
   * Clear any sorting that has been set on this node.
   *
   * @return Query Self, for message chaining
   */
  public function unsorted () {
    unset($this->sort);
    return $this;
  }

  /**
   * Add a query string to this node.
   * This method supports the full query string functionality of elastic search:
   * http://www.elasticsearch.org/guide/reference/query-dsl/query-string-query.html
   * https://lucene.apache.org/core/3_6_1/queryparsersyntax.html
   *
   * @param string $query The Query String to add
   * @param string $field Default field to search
   * @param string $op Default operator if no explicit operator is specified
   * @return Query Self, for message chaining
   */
  public function queryString ($query, $field = null, $op = null) {
    $q = $this->query->filtered->query();
    if (null == $query || '*' == $query) {
      $q->match_all = new Query();
    } else {
      $q->query_string->query = $query;
      if (null !== $field) {
        $q->query_string->default_field = $field;
      }
      if (null !== $op) {
        $q->query_string->default_operator = $op;
      }
    }

    return $this;
  } //end queryString

  /**
   * Add a term filter to this node.
   *
   * A term filter matches documents having an exact match in the given field.
   *
   * @param string $field Field to check for term
   * @param mixed $term Term to require
   * @return Query Self, for message chaining
   */
  public function termFilter ($field, $term) {
    if (null !== $term && '' !== $term) {
      $this->term->{$field} = $term;
    }
    return $this;
  } //end termFilter

  /**
   * Add a missing filter to this node.
   *
   * A missing filter matches documents that do not contain the given field.
   *
   * @param string $field Field to check
   * @return Query Self, for message chaining
   */
  public function missingFilter ($field) {
    $this->missing->field = $field;
    return $this;
  } //end missingFilter

  /**
   * Add a range filter to this node.
   *
   * @param string $field Field to check for range
   * @param mixed $from Range start
   * @param mixed $to Range end
   * @param bool $includeLower Should lower bound be inclusive? (>=)
   * @param bool $includeUpper Should upper bound be inclusive? (<=)
   * @return Query Self, for message chaining
   */
  public function rangeFilter (
      $field, $from, $to, $includeLower = true, $includeUpper = true) {
    $this->range->{$field}->_range($from, $to, $includeLower, $includeUpper);
    return $this;
  } //end rangeFilter

  /**
   * Add a range facet to this node.
   *
   * @param string $name Facet name
   * @param string $field Field to compute facet on
   * @param array $ranges Range limits as (low, high) pairs
   * @return Query Self, for message chaining
   */
  public function rangeFacet ($name, $field, $ranges) {
    $rangeList = array();
    foreach ($ranges as $range) {
      $r = new Query();
      $rangeList[] = call_user_func_array(array($r, '_range'), $range);
    }
    if (!empty($rangeList)) {
      $this->facets->{$name}->range->{$field} = $rangeList;
    }

    return $this;
  } //end rangeFacet

  /**
   * Add a terms facet to this node.
   *
   * @param string $name Facet name
   * @param string $field Field to compute facet on
   * @param int $size Return top N terms (null for all)
   * @param array $parms Additional parameters to add to facet
   * @return Query Self, for message chaining
   * @see http://www.elasticsearch.org/guide/reference/api/search/facets/terms-facet.html
   */
  public function termsFacet ($name, $field, $size = null, $parms = array()) {
    $facet = $this->facets->{$name}->terms();
    $facet->field = $field;
    if (null === $size) {
      $facet->all_terms = true;
    } else {
      $facet->size = $size;
    }

    foreach ($parms as $key => $val) {
      $facet->{$key} = $val;
    }

    return $this;
  } //end termsFacet

  /**
   * Add a date histogram facet to this node.
   *
   * @param string $name Facet name
   * @param string $field Field to compute facet on
   * @param string $interval Histogram bucket width
   * @param array $parms Additional parameters to add to facet
   * @return Query Self, for message chaining
   * @see http://www.elasticsearch.org/guide/reference/api/search/facets/date-histogram-facet.html
   */
  public function dateHistogramFacet (
      $name, $field, $interval = 'hour', $parms = array()) {
    $facet = $this->facets->{$name}->date_histogram();
    $facet->field = $field;
    $facet->interval = $interval;

    foreach ($parms as $key => $val) {
      $facet->{$key} = $val;
    }

    return $this;
  } //end dateHistogramFacet

  /**
   * Add a statistical facet to this node.
   *
   * @param string $name Facet name
   * @param string $field Field to compute facet on
   * @return Query Self, for message chaining
   * @see http://www.elasticsearch.org/guide/reference/api/search/facets/statistical-facet.html
   */
  public function statsFacet ($name, $field) {
    $this->facets->{$name}->statistical->field = $field;
    return $this;
  } //end statsFacet

  /**
   * Add a range clause to this node.
   *
   * @param mixed $from Range start
   * @param mixed $to Range end
   * @param bool $includeLower Should lower bound be inclusive? (>=)
   * @param bool $includeUpper Should upper bound be inclusive? (<=)
   * @return Query Self, for message chaining
   */
  protected function _range (
      $from, $to, $includeLower = true, $includeUpper = true) {
    if (null !== $from && '' !== $from) {
      $this->from = static::_cast($from);
    }
    if (null !== $to && '' !== $to) {
      $this->to = static::_cast($to);
    }
    $this->include_lower = $includeLower;
    $this->include_upper = $includeUpper;

    return $this;
  } //end _range

  /**
   * Cast a value for inclusion in a query.
   *
   * @param mixed $val Value to cast
   * @return mixed Cast value
   */
  protected static function _cast ($val) {
    if ($val instanceof \DateTime) {
      $val = $val->format('c');
    }
    return $val;
  }

  /**
   * Append the result of a dynamic method call to an array property of this
   * instance.
   *
   * @param string $list List to append to
   * @param string $method Method to call for value
   * @param array $args Call arguments
   * @return Query Self, for message chaining
   * @throws \BadMethodCallException If proxy lookup fails
   */
  protected function _listAppend ($list, $method, $args) {
    if (!method_exists($this, $method)) {
      throw new \BadMethodCallException(
          "Method Query::{$method} does not exist.");
    }

    // create array if needed
    if (!isset($this->{$list})) {
      $this->{$list} = array();
    }

    $child = new Query();
    $this->{$list}[] = call_user_func_array(array($child, $method), $args);
    return $this;
  } //end _listAppend

  /**
   * Abuse the magic helper for reading inaccessible properties.
   *
   * Default php behavior is to prentend as though a stdClass instance exists
   * when an assignment call includes one or more undefined properties in it's
   * variable name. By overloading this method we can tweak this behavior to
   * return another Query object instead which will keep our helper
   * methods available as we build the object graph.
   *
   * @param string $name Property name
   * @return Query Newly allocated Query instance
   */
  public function __get ($name) {
    $this->{$name} = new Query();
    return $this->{$name};
  } //end __get

  /**
   * Abuse the magic helper for writing inaccessible properties.
   *
   * If value being set is an Query instance, decorate it with a
   * pointer to the parent object it is being added to and the name of the
   * property slot it is being stored in.
   *
   * @param string $name Property name
   * @param mixed $value Value to store
   * @return void
   */
  public function __set ($name, $value) {
    if ($value instanceof Query) {
      // tell our new child who we are and what we call her.
      $value->_parent = $this;
      $value->_slot = $name;
    }

    $this->{$name} = $value;
  } //end __set

  /**
   * List prefixes that will trigger `__call` magic handling.
   * @var array
   */
  protected static $_listNames = array('and', 'or');

  /**
   * Attempt to resolve undefined method calls as list creation helper
   * methods.
   *
   * @param string $name Method name
   * @param array $args Call arguments
   * @return mixed Call result
   * @throws \BadMethodCallException If proxy lookup fails
   */
  public function __call ($name, $args) {
    // is the call a list append operation?
    foreach (static::$_listNames as $list) {
      if (0 === mb_stripos($name, $list)) {
        return $this->_listAppend(
            $list, mb_substr($name, mb_strlen($list)), $args);
      }
    }

    // is it a property check?
    if (0 === mb_stripos($name, 'has')) {
      $name = mb_substr($name, 3);
      $name = mb_strtolower(mb_substr($name, 0, 1)) .  mb_substr($name, 1);
      return $this->has($name);
    }

    // how about a property set disguised as a func call?
    if (empty($args)) {
      // create and return new empty node with the given name
      $this->{$name} = new Query();
      return $this->{$name};

    } else if (1 == count($args)) {
      // assign the given value to a property on this node
      $this->{$name} = $args[0];
      return $this;
    }

    // fall through to an error
    throw new \BadMethodCallException(
          "Method Query::{$name} does not exist.");
  } //end __call

  /**
   * Abuse the ArrayAccess::offsetSet method to replace ourself in our parent
   * object with a native php array.
   *
   * Why would we want to do this? Well because we are trying to keep some
   * seriously sneaky magic that php does for stdClass type objects working
   * while implementing a custom type that can do some other good things.
   * Assigning to an unset variable as though it is an array is perfectly
   * legal php syntax, but our `__get` magic trips it up. This hack puts that
   * behavior back in place via some slightly sneaky slight of hand.
   *
   * @param mixed $offset Array index to populate
   * @param mixed $value Value to assign to array slot
   * @return void
   * @see ArrayAccess
   * @throws \DomainException If this node doesn't know how to swap itself for
   * an array in it's parent.
   */
  public function offsetSet ($offset, $value) {
    if (!$this->hasParent()) {
      throw new \DomainException(
          'Array assignment only available on nodes with a parent');
    }
    // replace ourself with an empty array in the parent
    $this->_parent->{$this->_slot} = array();

    if (null === $offset) {
      // caller used the `[]` append-to-array syntax
      $this->_parent->{$this->_slot}[] = $value;

    } else {
      // caller provided a specific index to set
      $this->_parent->{$this->_slot}[$offset] = $value;
    }
  } //end offsetSet

  /**
   * Stub to complete ArrayAccess interface.
   * @param mixed $offset Ignored
   * @return bool Always returns false.
   * @see ArrayAccess
   */
  public function offsetExists ($offset) {
    return false;
  }

  /**
   * Stub to complete ArrayAccess interface.
   * @param mixed $offset Ignored
   * @return mixed Always returns null.
   * @see ArrayAccess
   */
  public function offsetGet ($offset) {
    return null;
  }

  /**
   * Stub to complete ArrayAccess interface.
   * @param mixed $offset Ignored
   * @return void
   * @see ArrayAccess
   */
  public function offsetUnset ($offset) {
    // not implemented
  }

} //end Query
