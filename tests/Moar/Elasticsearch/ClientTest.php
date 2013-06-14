<?php
/**
 * @package Moar\Elasticsearch
 */

namespace Moar\Elasticsearch;

/**
 * @package Moar\Elasticsearch
 * @copyright 2013 Bryan Davis and contributors. All Rights Reserved.
 */
class ClientTest extends \PHPUnit_Framework_TestCase {

  protected $envUrl;

  public function setUp () {
    // store url from env
    $this->envUrl = getenv('ELASTICSEARCH_URL');
    // purge url from env
    putenv('ELASTICSEARCH_URL');
  }

  public function tearDown () {
    if (false !== $this->envUrl) {
      // restore prior env
      putenv("ELASTICSEARCH_URL={$this->envUrl}");
    }
  }

  public function test_default_constructor () {
    $c = new Client();
    $this->assertEquals(Client::DEFAULT_SERVER, $c->server());
    $this->assertNull($c->index());
    $this->assertNull($c->type());
  }

  public function test_constructor_index_array () {
    $c = new Client(null, array('foo', 'bar'));
    $this->assertEquals('foo,bar', $c->index());
  }

  public function test_constructor_type_array () {
    $c = new Client(null, null, array('foo', 'bar'));
    $this->assertEquals('foo,bar', $c->type());
  }

  public function test_connection_default () {
    $c = Client::connection();
    $this->assertInstanceOf('Moar\Elasticsearch\Client', $c);
    $this->assertEquals(Client::DEFAULT_SERVER, $c->server());
    $this->assertNull($c->index());
    $this->assertNull($c->type());
  }

  public function test_connection_full () {
    $c = Client::connection('http://127.0.0.1:1999/foo,bar/doc,inv');
    $this->assertInstanceOf('Moar\Elasticsearch\Client', $c);
    $this->assertEquals('http://127.0.0.1:1999', $c->server());
    $this->assertEquals('foo,bar', $c->index());
    $this->assertEquals('doc,inv', $c->type());
  }

  public function test_scan () {
    if (false === $this->envUrl) {
      $this->markTestSkipped('ELASTICSEARCH_URL not set in environment.');
    }

    $c = Client::connection($this->envUrl);
    // TODO: make fixture data set to load into ES
    $r = $c->scan(Query::newInstance()->query->match_all(), 100);

    $this->assertNotNull($r);
    $this->assertGreaterThan(0, $r->hits->total);

    $expectTotal = $r->hits->total;
    $got = 0;
    foreach ($r as $idx => $record) {
      $this->assertFalse($r->isError(), $idx);
      $got++;
    }
    $this->assertEquals($expectTotal, $got);
  }

} //end ClientTest
