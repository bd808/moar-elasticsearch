<?php
/**
 * @package Moar\Elasticsearch
 */

namespace Moar\Elasticsearch;

/**
 * @package Moar\Elasticsearch
 * @copyright 2013 Bryan Davis and contributors. All Rights Reserved.
 */
class QueryTest extends \PHPUnit_Framework_TestCase {

  /**
   * Given a object graph made by setting values on a Query object
   * When the graph is traversed
   * Then each object node will be an instance of Query
   */
  public function test_chaining () {
    $root = new Query();
    $root->child->grandchild->greatgrandchild->prop = 1;
    $root->child2->grandchild2->greatgrandchild2->prop = 2;

    self::walkGraph(array($this, 'isEsQueryObjectCallback'), $root, "root");
  } //end test_chaining

  /**
   * Given an object graph made by setting values on a Query object
   * When the leaf node is assigned using array append syntax
   * Then an array is created
   * And the array is populated
   * And the json serialization is correct
   */
  public function test_array_member () {
    $root = new Query();
    $root->child->grandchild->greatgrandchild->and[] = 1;
    $root->child->grandchild->greatgrandchild->and[] = 2;

    self::walkGraph(array($this, 'isEsQueryObjectCallback'), $root, "root");
    $node = $root->child->grandchild->greatgrandchild;
    $this->assertInternalType('array', $node->and);
    $this->assertEquals(2, count($node->and));
    $this->assertEquals('{"child":{"grandchild":{"greatgrandchild":{"and":[1,2]}}}}', json_encode($root));
  } //end test_array_member


  /**
   * Given an object graph made by a Query object
   * When andTermFilter and orTermFilter are called
   * Then and & or collections are created
   * And they are filled with term filter nodes
   */
  public function test_termfilter_collections () {
    $root = new Query();

    $root->andTermFilter('foo', 1);
    $root->andTermFilter('bar', 2);

    $this->assertObjectHasAttribute('and', $root);
    $this->assertInternalType('array', $root->and);
    foreach ($root->and as $idx => $node) {
      self::walkGraph(array($this, 'isEsQueryObjectCallback'),
          $node, "and[{$idx}]");
    }

    $root->orTermFilter('foo', 1);
    $root->orTermFilter('bar', 2);

    $this->assertObjectHasAttribute('or', $root);
    $this->assertInternalType('array', $root->or);
    foreach ($root->or as $idx => $node) {
      self::walkGraph(array($this, 'isEsQueryObjectCallback'),
          $node, "or[{$idx}]");
    }

    $this->assertEquals('{"and":[{"term":{"foo":1}},{"term":{"bar":2}}],"or":[{"term":{"foo":1}},{"term":{"bar":2}}]}', json_encode($root));
  } //end test_termfilter_collections


  /**
   * Given a dict of property names and values
   * When the newInstance() factory is called
   * Then a new Query is created
   * And the properties are set.
   */
  public function test_instance_factory () {
    $init = array(
        'from' => 0,
        'size' => 20,
        'fields' => array('_source'),
      );
    $root = Query::newInstance($init);

    foreach ($init as $slot => $val) {
      $this->assertObjectHasAttribute($slot, $root);
      $this->assertSame($val, $root->{$slot});
    }
  } //end test_instance_factory


  public function test_andterms () {
    $layerMap = array(
      'layer1' => 1,
      'layer2' => 2,
      'layer3' => 3,
      'layer4' => 4,
      'layer5' => 5,
    );

    $root = Query::andTerms($layerMap);

    $this->assertObjectHasAttribute('and', $root);
    $this->assertInternalType('array', $root->and);
    foreach ($root->and as $idx => $node) {
      self::walkGraph(array($this, 'isEsQueryObjectCallback'),
          $node, "and[{$idx}]");
    }

    $this->assertEquals('{"and":[{"term":{"layer1":1}},{"term":{"layer2":2}},{"term":{"layer3":3}},{"term":{"layer4":4}},{"term":{"layer5":5}}]}', $root->json());
  } //end test_andterms

  public function test_orterms () {
    $layerMap = array(
      'layer1' => 1,
      'layer2' => 2,
      'layer3' => 3,
      'layer4' => 4,
      'layer5' => 5,
    );

    $root = Query::orTerms($layerMap);

    $this->assertObjectHasAttribute('or', $root);
    $this->assertInternalType('array', $root->or);
    foreach ($root->or as $idx => $node) {
      self::walkGraph(array($this, 'isEsQueryObjectCallback'),
          $node, "or[{$idx}]");
    }

    $this->assertEquals('{"or":[{"term":{"layer1":1}},{"term":{"layer2":2}},{"term":{"layer3":3}},{"term":{"layer4":4}},{"term":{"layer5":5}}]}', $root->json());
  } //end test_orterms


  public function test_sort () {
    $root = Query::newInstance();
    $root->sort("date_entered");

    $this->assertEquals('{"sort":[{"date_entered":{"order":"asc"}}]}',
        $root->json());

    // unsorted should remove all traces of sort
    $root->unsorted();
    $this->assertEquals('{}', $root->json());
  } //end test_sort

  public function test_has () {
    $root = new Query();
    $this->assertFalse($root->has('xyzzy'), "has(xyzzy)");
    $this->assertFalse($root->hasXyzzy(), "hasXyzzy()");

    $root->xyzzy = 1;
    $this->assertTrue($root->has('xyzzy'), "has(xyzzy)");
    $this->assertTrue($root->hasXyzzy(), "hasXyzzy()");
  }

  public function test_missingfilter () {
    $root = Query::newInstance();
    $root->missingFilter('xyzzy');

    $this->assertEquals('{"missing":{"field":"xyzzy"}}', $root->json());
  }

  public function test_rangeFilter () {
    $root = Query::newInstance();
    $root->rangeFilter('xyzzy', 1, 10);

    $this->assertEquals('{"range":{"xyzzy":{"from":1,"to":10,"include_lower":true,"include_upper":true}}}', $root->json());
  }

  public function test_rangefacet () {
    $root = Query::newInstance();
    $root->rangeFacet('score', 'score', array(
        array(0, 9),
        array(10, 19),
      ));

    $this->assertEquals('{"facets":{"score":{"range":{"score":[{"from":0,"to":9,"include_lower":true,"include_upper":true},{"from":10,"to":19,"include_lower":true,"include_upper":true}]}}}}',
        $root->json());
  }

  public function test_termsfacet () {
    $root = Query::newInstance();
    $root->termsFacet('status', 'status');
    $root->termsFacet('site', 'site_id', 10);

    $this->assertEquals('{"facets":{"status":{"terms":{"field":"status","all_terms":true}},"site":{"terms":{"field":"site_id","size":10}}}}',
        $root->json());
  }

  public function test_datehistogramfacet () {
    $root = Query::newInstance();
    $root->dateHistogramFacet('date', 'transaction_date', 'hour');

    $this->assertEquals('{"facets":{"date":{"date_histogram":{"field":"transaction_date","interval":"hour"}}}}', $root->json());
  }

  public function test_statsfacet () {
    $root = Query::newInstance();
    $root->statsFacet('foo', 'xyzzy');

    $this->assertEquals('{"facets":{"foo":{"statistical":{"field":"xyzzy"}}}}',
        $root->json());
  }

  public function test_call_and_listappend () {
    $root = Query::newInstance();
    $root->andTermFilter('merchant_id', '999999');

    $this->assertEquals('{"and":[{"term":{"merchant_id":"999999"}}]}', $root->json());
  }

  public function test_call_and_setter () {
    $root = Query::newInstance();
    $root->foo('bar');
    $root->baz();

    $this->assertEquals('{"foo":"bar","baz":{}}', $root->json());
  }

  public function test_call_and_has () {
    $root = Query::newInstance();
    $root->foo = 1;

    $this->assertTrue($root->hasFoo());
    $this->assertFalse($root->hasBar());
  }

  /**
   * Callback to check that the given object is an instance of
   * Query.
   *
   * @param object $obj Object to check
   * @param string $path Path to object (used for error massage)
   */
  protected function isEsQueryObjectCallback ($obj, $path) {
    $this->assertInstanceOf('Moar\Elasticsearch\Query', $obj,
        "Expected <{$path}> to be a Query object");
  }

  /**
   * Walk an object graph and apply the given callback at each node.
   *
   * Performs a depth-first traversal of the given object graph calling the
   * callback function with each node that is found.
   *
   * The callback will be passed two parameters. The first will be the node
   * currently being visited. The second will be the path from the root of the
   * graph to the node as a dot separated string.
   *
   * @param callable $callback Callback to apply to each node
   * @param object $root Root of graph
   * @return void
   */
  protected static function walkGraph ($callback, $root, $path = null) {
    if (is_object($root)) {
      // apply the callback to the root node
      call_user_func($callback, $root, $path);

      // we may have children
      foreach ($root as $slot => $child) {
        if (is_object($child)) {
          // recurse to each child
          self::walkGraph($callback, $child, "{$path}.{$slot}");
        }
      }
    }
  } //end walkGraph

} //end QueryTest
