<?php


namespace Spartaques\CoreKafka\Consume\HighLevel\VendorExtends;


use Symfony\Component\Console\Formatter\OutputFormatterStyle;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Output\ConsoleOutput;

/**
 * Class Output
 * @package Spartaques\CoreKafka\Consume\HighLevel\Vendor
 */
class Output extends ConsoleOutput
{

    /**
     * @param $headers
     * @param array $rows
     * @param string $tableStyle
     * @param array $columnStyles
     */
    public function table($headers, array $rows, $tableStyle = 'default', array $columnStyles = [])
    {
        $table = new Table($this);

        $table->setHeaders((array) $headers)->setRows($rows)->setStyle($tableStyle);

        foreach ($columnStyles as $columnIndex => $columnStyle) {
            $table->setColumnStyle($columnIndex, $columnStyle);
        }

        $table->render();
    }


    /**
     * @param $string
     * @param null $verbosity
     */
    public function info($string, $verbosity = null)
    {
        $this->line($string, 'info', $verbosity);
    }


    /**
     * @param $string
     * @param null $style
     * @param null $verbosity
     */
    public function line($string, $style = null, $verbosity = null)
    {
        $styled = $style ? "<$style>$string</$style>" : $string;

        $this->writeln($styled);
    }


    /**
     * @param $string
     * @param null $verbosity
     */
    public function comment($string, $verbosity = null)
    {
        $this->line($string, 'comment', $verbosity);
    }


    /**
     * @param $string
     * @param null $verbosity
     */
    public function question($string, $verbosity = null)
    {
        $this->line($string, 'question', $verbosity);
    }


    /**
     * @param $string
     * @param null $verbosity
     */
    public function error($string, $verbosity = null)
    {
        $this->line($string, 'error', $verbosity);
    }


    /**
     * @param $string
     * @param null $verbosity
     */
    public function warn($string, $verbosity = null)
    {
        if (! $this->getFormatter()->hasStyle('warning')) {
            $style = new OutputFormatterStyle('yellow');

            $this->getFormatter()->setStyle('warning', $style);
        }

        $this->line($string, 'warning', $verbosity);
    }
}
