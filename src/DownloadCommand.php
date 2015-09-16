<?php

namespace Elazar\OdmpScraper;

use Goutte\Client;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DomCrawler\Crawler;
use SQLite3;

class DownloadCommand extends Command
{
    protected $db;

    protected $insert;

    protected $states = [
        'Alabama' => 'AL',
        'Alaska' => 'AK',
        'Arizona' => 'AZ',
        'Arkansas' => 'AR',
        'California' => 'CA',
        'Colorado' => 'CO',
        'Connecticut' => 'CT',
        'Delaware' => 'DE',
        'Florida' => 'FL',
        'Georgia' => 'GA',
        'Hawaii' => 'HI',
        'Idaho' => 'ID',
        'Illinois' => 'IL',
        'Indiana' => 'IN',
        'Iowa' => 'IA',
        'Kansas' => 'KS',
        'Kentucky' => 'KY',
        'Louisiana' => 'LA',
        'Maine' => 'ME',
        'Maryland' => 'MD',
        'Massachusetts' => 'MA',
        'Michigan' => 'MI',
        'Minnesota' => 'MN',
        'Mississippi' => 'MS',
        'Missouri' => 'MO',
        'Montana' => 'MT',
        'Nebraska' => 'NE',
        'Nevada' => 'NV',
        'New Hampshire' => 'NH',
        'New Jersey' => 'NJ',
        'New Mexico' => 'NM',
        'New York' => 'NY',
        'North Carolina' => 'NC',
        'North Dakota' => 'ND',
        'Ohio' => 'OH',
        'Oklahoma' => 'OK',
        'Oregon' => 'OR',
        'Pennsylvania' => 'PA',
        'Rhode Island' => 'RI',
        'South Carolina' => 'SC',
        'South Dakota' => 'SD',
        'Tennessee' => 'TN',
        'Texas' => 'TX',
        'Utah' => 'UT',
        'Vermont' => 'VT',
        'Virginia' => 'VA',
        'Washington' => 'WA',
        'West Virginia' => 'WV',
        'Wisconsin' => 'WI',
        'Wyoming' => 'WY',
        'American Samoa' => 'AS',
        'District of Columbia' => 'DC',
        'Federated States of Micronesia' => 'FM',
        'Guam' => 'GU',
        'Marshall Islands' => 'MH',
        'Northern Mariana Islands' => 'MP',
        'Palau' => 'PW',
        'Puerto Rico' => 'PR',
        'Virgin Islands' => 'VI',
    ];

    protected function configure()
    {
        $this
            ->setName('download')
            ->setDescription('Download the odmp.org data set')
            ->addOption(
                'output',
                null,
                InputOption::VALUE_OPTIONAL,
                'Path to a SQLite3 database to receive output'
            )
            ->addOption(
                'start',
                null,
                InputOption::VALUE_OPTIONAL,
                'Starting year'
            )
            ->addOption(
                'end',
                null,
                InputOption::VALUE_OPTIONAL,
                'Ending year'
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $path = $input->getOption('output');
        if ($path && substr($path, -3) !== '.db') {
            $output->writeln('<error>--output does not appear to reference a SQLite file path</error>');
            return 1;
        } else {
            $path = getcwd() . '/odmp.db';
        }

        if (!is_writable($path)) {
            $output->writeln('<error>--output does not appear to reference a writable path</error>');
            return 1;
        }

        $year = (int) $input->getOption('start') ?: 1791;
        $end_year = (int) $input->getOption('end') ?: (int) date('Y');
        $page_size = 25;
        $db = $this->getDb($path);
        while ($year <= $end_year) {
            $offset = 0;
            do {
                $crawler = $this->getCrawler(['from' => $year, 'to' => $year, 'o' => $offset]);
                $ending = $this->getEndingOffset($crawler);
                $results = $this->getResultsPage($crawler, $ending);
                $pages = $ending ? ceil($ending / $page_size) : 1;
                $page = ($offset / $page_size) + 1;
                $output->writeln('<info>Processing year ' . $year . ' page ' . $page . ' of ' . $pages . '</info>');
                foreach ($results as $result) {
                    $this->storeResult($db, $result);
                }
                $offset += $page_size;
            } while ($offset < $ending);
            $year++;
        }
    }

    protected function storeResult(SQLite3 $db, array $result)
    {
        $this->insert->clear();
        foreach ($result as $key => $value) {
            $this->insert->bindValue($key, $value, \SQLITE3_TEXT);
        }
        $this->insert->execute();
    }

    protected function getDb($path)
    {
        if (!$this->db) {
            $this->db = $db = new SQLite3($path);
            $db->exec('
                create table if not exists officers (
                    url text,
                    name text,
                    state text,
                    eow text,
                    cause text
                )
            ');
            $db->exec('delete from officers');
            $this->insert = $db->prepare('
                insert into officers (
                    url,
                    name,
                    state,
                    eow,
                    cause
                )
                values (
                    :url,
                    :name,
                    :state,
                    :eow,
                    :cause
                )
            ');
        }
        return $this->db;
    }

    protected function getResultsPage(Crawler $crawler, $ending)
    {
        if ($ending === null) {
            return strpos($crawler->html(), 'No Matches Found') === false
                ? $this->getSingleResultPage($crawler)
                : [];
        }
        $table = $crawler->filterXPath('//div[@id="pagination"]/../div/table');
        return $this->getMultipleResultsPage($table);
    }

    protected function getMultipleResultsPage(Crawler $crawler)
    {
        $results = [];
        $cells = $crawler->filterXPath('//tr/td[2]');
        foreach ($cells as $cell_node) {
            $cell = new Crawler($cell_node);
            $url = $cell->filterXPath('//a/@href')->text();
            $name = $cell->filterXPath('//a/text()')->text();
            $cell_html = $cell->html();
            $cell_lines = preg_split('#<br>(</br>)?\s+#m', $cell_html);
            if (isset($cell_lines[4]) && strpos($cell_lines[4], 'Location') !== false) {
                $state_name = trim(str_replace('Location: ', '', $cell_lines[4]));
                $state = $this->states[$state_name];
            } elseif (strpos($cell_lines[3], 'Location') !== false) {
                $state_name = trim(preg_replace(['/Location: /', '/\s*<.+$/'], '', strstr($cell_lines[3], 'Location')));
                $state = isset($this->states[$state_name]) ? $this->states[$state_name] : null;
            } elseif (preg_match('/[A-Z]{2}$/', $cell_lines[1], $match)) {
                $state = $match[0];
            } else {
                $state = null;
            }
            $eow = (new \DateTime(str_replace('EOW: ', '', $cell_lines[2])))->format('Y-m-d');
            $cause = preg_replace(['/Cause of Death: /', '#\s*<.+$#'], '', $cell_lines[3]);
            $results[] = [
                ':url' => $url,
                ':name' => $name,
                ':state' => $state,
                ':eow' => $eow,
                ':cause' => $cause,
            ];
        }
        return $results;
    }

    protected function getSingleResultPage(Crawler $crawler)
    {
        $header = $crawler->filterXPath('//div[@id="memorial_featuredInfo_right"]');
        $url = $crawler->filterXPath('//meta[@property="og:url"]/@content')->text();
        $name = $header->filterXPath('//h4')->text() . ' ' . $header->filterXPath('//h3')->text();
        if (preg_match('/, ([^,]+)$/', $crawler->filterXPath('//title')->text(), $match)
            && isset($this->states[$match[1]])) {
            $state_name = $match[1];
        } else {
            $p = $crawler->filterXPath('//b[contains(text(), "Location:")]/..');
            if (count($p)) {
                $state_name = str_replace('Location: ', '', $p->text());
            }
        }
        $state = empty($state_name) ? null : $this->states[$state_name];
        $eow = preg_match('/End of Watch: ([^<]+)/', $header->html(), $match)
            ? (new \DateTime(trim($match[1])))->format('Y-m-d')
            : null;
        $cause = str_replace('Cause: ', '', $crawler->filterXPath('//b[contains(text(), "Cause:")]/..')->text());
        $result = [[
            ':url' => $url,
            ':name' => $name,
            ':state' => $state,
            ':eow' => $eow,
            ':cause' => $cause,
        ]];
        return $result;
    }

    protected function getEndingOffset(Crawler $crawler)
    {
        $p = $crawler->filterXPath('//p[contains(text(), "Displaying officers")]');
        if (!count($p)) {
            return null;
        }
        $p_text = $p->text();
        if (preg_match('/of ([0-9]+)/', $p_text, $match)
            || preg_match('/through ([0-9]+)/', $p_text, $match)) {
            return (int) $match[1];
        }
        return null;
    }

    protected function getCrawler(array $overrides = [])
    {
        $base_url = 'https://www.odmp.org/search';
        $defaults = [
            'name' => '',
            'agency' => '',
            'state' => '',
            'from' => '1791',
            'to' => date('Y'),
            'cause' => '',
            'filter' => 'all',
        ];
        $parameters = array_merge($defaults, $overrides);
        $url = $base_url . '?' . http_build_query($parameters);
        $client = new Client;
        return $client->request('GET', $url);
    }
}
