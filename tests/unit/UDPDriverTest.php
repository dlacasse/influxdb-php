<?php
namespace InfluxDB\Test\unit;

use PHPUnit\Framework\TestCase;
use InfluxDB\Driver\UDP;

class UDPDriverTest extends TestCase
{
    /**
     * Check that constructor arguments are properly set into internal config property
     *
     * @throws \ReflectionException
     */
    public function testConstructor() {
        $udpDriver = new UDP('localhost', 0);

        $reflector = new \ReflectionProperty(UDP::class, 'config');
        $reflector->setAccessible(true);
        $configData = $reflector->getValue($udpDriver);

        $this->assertArrayHasKey('host', $configData, 'Ensure host key exists');
        $this->assertEquals('localhost', $configData['host'], 'Ensure host value set correctly');

        $this->assertArrayHasKey('port', $configData, 'Ensure port key exists');
        $this->assertEquals(0, $configData['port'], 'Ensure host value set correctly');

        $this->assertArrayHasKey('chunkSize', $configData, 'Ensure chunkSize key exists');
        $this->assertEquals('60000', $configData['chunkSize'], 'Ensure host value set correctly');

        $this->assertArrayHasKey('lineSeparator', $configData, 'Ensure lineSeparator key exists');
        $this->assertEquals(PHP_EOL, $configData['lineSeparator'], 'Ensure host value set correctly');
    }

    /**
     * Test for getAddressString()
     *
     * @throws \ReflectionException
     */
    public function testGetAddressString() {
        $udpDriver = new UDP('localhost', 0);

        $reflection = new \ReflectionClass(UDP::class);
        $method = $reflection->getMethod('getAddressString');
        $method->setAccessible(true);

        $address = $method->invokeArgs($udpDriver, []);

        $this->assertEquals('udp://localhost:0', $address, 'Ensure that address was constructed properly');
    }

    /**
     * Test that writing works as expected (payloads which exceed the size limit should trigger multiple sends)
     *
     * @param string $data      Payload data
     * @param int    $sizeLimit Limit of payload size
     * @param int    $expected  The number of times we expect send to be called.
     *
     * @dataProvider payloadProvider
     */
    public function testWrite($data, $sizeLimit, $expected) {
        $mock = $this->getMockBuilder(UDP::class)
            ->setConstructorArgs(['host' => '127.0.0.1', 'port' => 0, 'chunkSize' => $sizeLimit])
            ->setMethods(['send', 'createStream'])
            ->getMock();

        $mock->expects($this->exactly($expected))->method('send');

        $mock->write($data);
    }

    /**
     *
     *
     * @return array
     */
    public function payloadProvider()
    {
        return [
            'empty dataset' => [
                '',
                100,
                0
            ],
            'single line' => [
                'production.example.processing_time,host=server1,datacenter=east,process_id=1,app=null,type=gauge value=0 1533321645028142080',
                100,
                1
            ],
            'single line with ending' => [
                'production.example.processing_time,host=server1,datacenter=east,process_id=2,app=null,type=gauge value=0 1533321645028142080' . PHP_EOL,
                100,
                1
            ],
            'multi line' => [
                'production.example.processing_time,host=server1,datacenter=east,process_id=2,app=null,type=gauge value=0 1533321644968761088' . PHP_EOL .
                'production.example.processing_time,host=server1,datacenter=east,process_id=3,app=null,type=gauge value=0 1533321644968905984',
                100,
                2
            ],
            'from file' => [
                file_get_contents(__DIR__ . '/text/sample.txt'),
                60000,
                2
            ]
        ];
    }
}