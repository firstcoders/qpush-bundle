<?php
namespace Uecode\Bundle\QPushBundle\Provider;

use Doctrine\Common\Cache\Cache;
use Symfony\Bridge\Monolog\Logger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;
use Uecode\Bundle\QPushBundle\Event\MessageEvent;
use Uecode\Bundle\QPushBundle\Message\Message;

class FileProvider extends AbstractProvider
{
    protected $filePointerList = [];

    public function __construct($name, array $options, $client, Cache $cache, Logger $logger) {
        $this->name     = $name;
        $this->options  = $options;
        $this->cache    = $cache;
        $this->logger   = $logger;
    }

    public function getProvider()
    {
        return 'File';
    }

    public function create()
    {
        $fs = new Filesystem();
        if (!$fs->exists($this->options['path'])) {
            return $fs->mkdir($this->options['path']);
        }
        return true;
    }

    public function publish(array $message, array $options = [])
    {
        $fileName = microtime(false);
        $fileName = str_replace(' ', '', $fileName);
        $path = substr(hash('md5', $fileName), 0, 3);
        if (!is_dir($this->options['path'].DIRECTORY_SEPARATOR.$path)) {
            mkdir($this->options['path'].DIRECTORY_SEPARATOR.$path);
        }
        $fs = new Filesystem();
        $fs->dumpFile(
            $this->options['path'].DIRECTORY_SEPARATOR.$path.DIRECTORY_SEPARATOR.$fileName.'.json',
            json_encode($message)
        );
        return $fileName;
    }

    /**
     * @param array $options
     * @return Message[]
     */
    public function receive(array $options = [])
    {
        $finder = new Finder();
        $finder
            ->files()
            ->ignoreDotFiles(true)
            ->ignoreUnreadableDirs(true)
            ->ignoreVCS(true)
            ->name('*.json')
            ->in($this->options['path'])
        ;
        if ($this->options['message_delay'] > 0) {
            $finder->date(
                sprintf('< %s', $this->convertSecondToHuman($this->options['message_delay']))
            );
        }
        $finder
            ->date(
                sprintf('> %s', $this->convertSecondToHuman($this->options['message_expiration']))
            )
        ;
        $messages = [];
        /** @var SplFileInfo $file */
        foreach ($finder as $file) {
            $filePointer = fopen($file->getRealPath(), 'r+');
            $id = substr($file->getFilename(), 0, -5);
            if (!isset($this->filePointerList[$id]) && flock($filePointer, LOCK_EX | LOCK_NB)) {
                $this->filePointerList[$id] = $filePointer;
                 $messages[] = new Message($id, json_decode($file->getContents(), true), []);
            } else {
                fclose($filePointer);
            }
            if (count($messages) === (int) $this->options['messages_to_receive']) {
                break;
            }
        }
        return $messages;
    }

    private function convertSecondToHuman($seconds) {
        if ($seconds / 60 >= 1) {
            $minutes = floor($seconds / 60);
            $seconds = $seconds % 60;
        } else {
            return sprintf('%d seconds ago', $seconds);
        }
        if ($minutes / 60 >= 1) {
            $hours = floor($minutes / 60);
            $minutes = $minutes % 60;
        } else {
            return sprintf('%d minutes %d seconds ago', $minutes, $seconds);
        }
        if ($hours / 24 >= 1) {
            $days = floor($hours / 24);
            $hours = $hours % 24;
        } else {
            return sprintf('%d hours %d minutes %d seconds ago', $hours, $minutes, $seconds);
        }
        return sprintf('%d days %d hours %d minutes %d seconds ago', $days, $hours, $minutes, $seconds);
    }

    public function delete($id)
    {
        $success = false;
        if (isset($this->filePointerList[$id])) {
            $fileName = $id;
            $path = substr(hash('md5', (string)$fileName), 0, 3);
            $fs = new Filesystem();
            $fs->remove(
                $this->options['path'] . DIRECTORY_SEPARATOR . $path . DIRECTORY_SEPARATOR . $fileName . '.json'
            );
            fclose($this->filePointerList[$id]);
            unset($this->filePointerList[$id]);
            $success = true;
        }
        if (rand(1,10) === 5) {
            $this->cleanUp();
        }
        return $success;
    }

    public function cleanUp()
    {
        $finder = new Finder();
        $finder
            ->files()
            ->in($this->options['path'])
            ->ignoreDotFiles(true)
            ->ignoreUnreadableDirs(true)
            ->ignoreVCS(true)
            ->depth('< 2')
            ->name('*.json')
        ;
        $finder->date(
            sprintf('> %s', $this->convertSecondToHuman($this->options['message_expiration']))
        );
        /** @var SplFileInfo $file */
        foreach ($finder as $file) {
            @unlink($file->getRealPath());
        }
    }

    public function destroy()
    {
        $fs = new Filesystem();
        $fs->remove($this->options['path']);
        return !is_dir($this->options['path']);
    }

    /**
     * Removes the message from queue after all other listeners have fired
     *
     * If an earlier listener has erred or stopped propagation, this method
     * will not fire and the Queued Message should become visible in queue again.
     *
     * Stops Event Propagation after removing the Message
     *
     * @param MessageEvent $event The SQS Message Event
     * @return bool|void
     */
    public function onMessageReceived(MessageEvent $event)
    {
        $id = $event->getMessage()->getId();
        $this->delete($id);
        $event->stopPropagation();
    }
}