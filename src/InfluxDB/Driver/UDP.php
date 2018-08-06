<?php
/**
 * @author Stephen "TheCodeAssassin" Hoogendijk
 */

namespace InfluxDB\Driver;

/**
 * Class UDP
 *
 * @package InfluxDB\Driver
 */
class UDP implements DriverInterface
{
    /**
     * Parameters
     *
     * @var array
     */
    private $parameters;

    /**
     * @var array
     */
    private $config;

    /**
     *
     * @var resource
     */
    private $stream;

    /**
     * Constructor.
     *
     * @param string $host          IP/hostname of the InfluxDB host
     * @param int    $port          Port of the InfluxDB process
     * @param int    $chunkSize     Approximate size of UDP packets
     * @param string $lineSeparator Character used as the line separator
     */
    public function __construct($host, $port, $chunkSize = 60000, $lineSeparator = PHP_EOL)
    {
        $this->config['host'] = $host;
        $this->config['port'] = $port;
        $this->config['chunkSize'] = $chunkSize;
        $this->config['lineSeparator'] = $lineSeparator;
    }

    /**
     * Close the stream (if created)
     */
    public function __destruct()
    {
        if (isset($this->stream) && is_resource($this->stream)) {
            fclose($this->stream);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function setParameters(array $parameters)
    {
        $this->parameters = $parameters;
    }

    /**
     * {@inheritdoc}
     */
    public function getParameters()
    {
        return $this->parameters;
    }

    /**
     * {@inheritdoc}
     */
    public function write($data = null)
    {
        if (isset($this->stream) === false) {
            $this->createStream();
        }
        $this->chunkAndSendData($data);

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function isSuccess()
    {
        return true;
    }

    /**
     * Create the resource stream
     */
    protected function createStream()
    {
        // stream the data using UDP and suppress any errors
        $this->stream = @stream_socket_client($this->getAddressString());
    }

   /***
    * Using the host and port provided upon instantiation, generate the remote address string
    *
    * @return string
    */
    protected function getAddressString()
    {
        return sprintf('udp://%s:%d', $this->config['host'], $this->config['port']);
    }

    /**
     * Since the UDP protocol supports a maximum payload size of ~64kb, we need to chunk up the requests for larger
     * payloads. Since the payloads are line-delimted, we need to ensure that we don't split up an individual lines
     *
     * @param string $data
     *
     * @return void
     */
    protected function chunkAndSendData($data)
    {
        while(strlen($data) > 0)
        {
            $currentChunk = $data;

            if (strlen($data) > $this->config['chunkSize'])
            {
                $endOfChunk = strpos($data, $this->config['lineSeparator'], $this->config['chunkSize']);

                if ($endOfChunk) {
                    $currentChunk = substr($data, 0, $endOfChunk);
                }
            }
            $this->send($currentChunk);

            $data = substr($data, strlen($currentChunk) + 1);
        }
    }

  /**
   * Send the data to the stream
   *
   * @param $data
   *
   * @return void
   */
  protected function send($data)
  {
      @stream_socket_sendto($this->stream, $data);
  }
}
