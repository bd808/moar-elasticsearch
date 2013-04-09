<?php
/**
 * @package Moar\Elasticsearch
 */

namespace Moar\Elasticsearch;

/**
 * @package Moar\Elasticsearch
 * @copyright 2013 Bryan Davis and contributors. All Rights Reserved.
 */
class ResponseTest extends \PHPUnit_Framework_TestCase {

  public function test_member_copy () {
    $r = new Response('{"a":1,"b":{"c":2}}');
    $this->assertEquals(1, $r->a);
    $this->assertEquals(2, $r->b->c);
  }

  public function test_json () {
    $expect = '{"a":1,"b":{"c":2}}';
    $r = new Response($expect);
    $this->assertEquals($expect, json_encode($r));
  }

  public function test_construct_from_object () {
    $expect = new \stdClass;
    $expect->took = 7;
    $expect->hits = new \stdClass;
    $expect->hits->total = 100;
    $expect->hits->hits = array(0,1,2,3,4,5,6,7,8,9);

    $r = new Response($expect);

    foreach ($expect as $key => $val) {
      $this->assertEquals($val, $r->{$key});
    }
  }

  public function test_empty_is_error () {
    $r = new Response('');
    $this->assertTrue($r->isError());
  }

  /**
   * @param int $status Status code
   * @param bool $err Is status an error?
   *
   * @dataProvider statusProvider
   */
  public function test_status_codes ($status, $err) {
    $r = new Response('{"a":1}', $status);
    $this->assertEquals($err, $r->isError());
  }

  public function statusProvider () {
    return array(
        array(100, true),
        array(150, true),
        array(199, true),
        array(200, false),
        array(250, false),
        array(299, false),
        array(300, true),
        array(350, true),
        array(399, true),
      );
  } //end statusProvider


} //end ResponseTest
