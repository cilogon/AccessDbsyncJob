<?php

App::uses("CoJobBackend", "Model");

class AccessDbsyncJob extends CoJobBackend {
  // Required by COmanage Plugins
  public $cmPluginType = "job";

  // Document foreign keys
  public $cmPluginHasMany = array();

  // Validation rules for table elements
  public $validate = array();

  // Current CO Job Object
  private $CoJob;

  // Current CO ID
  private $coId;

  // Paging offset
  private $pagingOffset = null;

  // Paging limit
  private $pagingLimit = null;

  // Process only one page of profiles
  private $onePageOnly = null;

  // Synchronize this single ACCESS ID
  private $requestedAccessId = null;

  /**
   * Obtain the ACCESS ID from the User Database profile. Throw
   * an exception if the ACCESS ID cannot be found from the profile.
   *
   * @param  array $profile Profile returned from User Database
   * @return string
   * @throws RuntimeException
   */

  private function accessIdFromProfile($profile) {
    try {
      $accessId = $profile['username'];
    } catch (Exception $e) {
      $msg = 'Error obtaining ACCESS ID from profile: ';
      $msg = $msg . $e->getMessage();
      $msg = $msg . ' : profile is ' . print_r($profile, true);
      $this->log($msg);

      throw new RuntimeException($msg);
    }

    return $accessId;
  }

  /**
   * Expose menu items.
   * 
   * @return Array with menu location type as key and array of labels, controllers, actions as values.
   */
  public function cmPluginMenus() {
    return array();
  }

  /**
   * Create the ACCESS ID extended type.
   *
   * @return void
   */
  public function createAccessIdExtendedType() {
    $args = array();
    $args['conditions']['CoExtendedType.co_id'] = $this->coId;
    $args['conditions']['CoExtendedType.attribute'] = 'Identifier.type';
    $args['conditions']['CoExtendedType.name'] = 'accessid';
    $args['contain'] = false;

    $exType = $this->CoJob->Co->CoExtendedType->find("first", $args);
    if(!empty($exType)) {
      $this->log("ACCESS ID extended type exists");
      return;
    }

    $this->CoJob->Co->CoExtendedType->clear();

    $data = array();
    $data['CoExtendedType']['co_id'] = $this->coId;
    $data['CoExtendedType']['attribute'] = 'Identifier.type';
    $data['CoExtendedType']['name'] = 'accessid';
    $data['CoExtendedType']['display_name'] = 'ACCESS ID';
    $data['CoExtendedType']['status'] = SuspendableStatusEnum::Active;

    if(!$this->CoJob->Co->CoExtendedType->save($data)) {
      $this->log("Failed to save the ACCESS ID extended type " . print_r($data, true));
    }
  }

  /**
   * Create a new CO Person record from the User Database profile.
   * The new record has linked to it:
   *  - Identifier
   *      - type accessid and value ACCESS ID
   *  - Name (Primary)
   *  - EmailAddress
   *  - CoPersonRole
   *      - affiliation Affiliate and organization from profile
   *  - OrgIdentity
   *      - Identifier
   *          - ePPN with scope access-ci.org
   *          - login
   *  - Name (Primary)
   *  
   * @param  array $profile Profile returned from User Database
   * @return void
   * @throws RuntimeException
   */

  private function createCoPersonFromProfile($profile) {
    $accessId = $this->accessIdFromProfile($profile);

    // Begin a transaction.
    $dataSource = $this->CoJob->getDataSource();
    $dataSource->begin();

    // Create the CO Person object.
    $this->CoJob->Co->CoPerson->clear();

    $data = array();
    $data['CoPerson'] = array();
    $data['CoPerson']['co_id'] = $this->coId;
    $data['CoPerson']['status'] = StatusEnum::Active;

    $args = array();
    $args['provision'] = false;

    if(!$this->CoJob->Co->CoPerson->save($data, $args)) {
      $msg = "ERROR could not create CoPerson: ";
      $msg = $msg . "ACCESS ID $accessId";
      $msg = $msg . "ERROR CoPerson validation errors: ";
      $msg = $msg . print_r($this->CoJob->Co->CoPerson->validationErrors, true);
      $this->log($msg);
      $dataSource->rollback();
      throw new RuntimeException($msg);
    }

    // Record the CoPerson ID for later assignment.
    $coPersonId = $this->CoJob->Co->CoPerson->id;

    // Attach an Identifier with value ACCESS ID to the CoPerson.
    $this->CoJob->Co->CoPerson->Identifier->clear();

    $data = array();
    $data['Identifier'] = array();
    $data['Identifier']['identifier'] = $accessId;
    $data['Identifier']['type'] = 'accessid';
    $data['Identifier']['status'] = SuspendableStatusEnum::Active;
    $data['Identifier']['co_person_id'] = $coPersonId;

    // Dynamically disable the Identifier Validator for this CO
    // for the ACCESS ID.
    $args = array();
    $args['conditions']['CoIdentifierValidator.co_id'] = $this->coId;
    $args['conditions']['CoIdentifierValidator.status'] = SuspendableStatusEnum::Active;
    $args['contain'][] = 'CoExtendedType';

    $validators = $this->CoJob->Co->CoIdentifierValidator->find('all', $args);

    if(!empty($validators)) {
      foreach($validators as $v) {
        if($v['CoExtendedType']['name'] == 'accessid') {
          $accessIdValidatorId = $v['CoIdentifierValidator']['id'];
          $this->CoJob->Co->CoIdentifierValidator->id = $accessIdValidatorId;
          $this->CoJob->Co->CoIdentifierValidator->saveField('status', SuspendableStatusEnum::Suspended);
          break;
        }
      }
    }

    $args = array();
    $args['validate'] = false;
    $args['provision'] = false;

    try {
      $success = $this->CoJob->Co->CoPerson->Identifier->save($data, $args);
      if(!$success) {
        throw new RuntimeException();
      }
    } catch (Exception $e) {
      $msg = "ERROR could not create Identifier for CoPerson: ";
      $msg = $msg . "ACCESS ID $accessId";
      $this->log($msg);
      $dataSource->rollback();
      throw new RuntimeException($msg);
    } finally {
      if(!empty($accessIdValidatorId)) {
        $this->CoJob->Co->CoIdentifierValidator->id = $accessIdValidatorId;
        $this->CoJob->Co->CoIdentifierValidator->saveField('status', SuspendableStatusEnum::Active);
      }
    }

    // Attach a Name to the CoPerson record.
    $this->CoJob->Co->CoPerson->Name->clear();

    $data = $this->nameArrayFromProfile($profile);

    $data['Name']['type'] = NameEnum::Official;
    $data['Name']['primary_name'] = true;
    $data['Name']['co_person_id'] = $coPersonId;

    $args = array();
    $args['provision'] = false;

    if(!$this->CoJob->Co->CoPerson->Name->save($data, $args)) {
      $msg = "ERROR could not create Name for CoPerson: ";
      $msg = $msg . "ACCESS ID $accessId ";
      $msg = $msg . "ERROR Name validation errors: ";
      $msg = $msg . print_r($this->CoJob->Co->CoPerson->Name->validationErrors, true);
      $this->log($msg);
      $dataSource->rollback();
      throw new RuntimeException($msg);
    }

    // Attach an EmailAddress to the CoPerson record.
    $this->CoJob->Co->CoPerson->EmailAddress->clear();

    $data = array();
    $data['EmailAddress'] = array();
    $data['EmailAddress']['mail'] = (empty($profile['email'])) ? "placeholder" : $profile['email'];
    $data['EmailAddress']['description'] = 'Synched from central User Database';
    $data['EmailAddress']['type'] = EmailAddressEnum::Official;
    $data['EmailAddress']['verified'] = true;
    $data['EmailAddress']['co_person_id'] = $coPersonId;

    $options = array();
    $options['trustVerified'] = true;
    $options['provision'] = false;

    if(!$this->CoJob->Co->CoPerson->EmailAddress->save($data, $options)) {
      $msg = "ERROR could not create EmailAddress for CoPerson: ";
      $msg = $msg . "ACCESS ID $accessId ";
      $msg = $msg . "ERROR EmailAddress validation errors: ";
      $msg = $msg . print_r($this->CoJob->Co->CoPerson->EmailAddress->validationErrors, true);
      $this->log($msg);
      $dataSource->rollback();
      throw new RuntimeException($msg);
    }

    // Attach a CoPersonRole to the CoPerson.
    $this->CoJob->Co->CoPerson->CoPersonRole->clear();
    
    $data = array();
    $data['CoPersonRole'] = array();
    $data['CoPersonRole']['co_person_id'] = $coPersonId;
    $data['CoPersonRole']['status'] = StatusEnum::Active;
    $data['CoPersonRole']['affiliation'] = AffiliationEnum::Affiliate;
    $data['CoPersonRole']['o'] = (empty($profile['organizationName'])) ? "placeholder" : $profile['organizationName'];

    $args = array();
    $args['provision'] = false;

    if(!$this->CoJob->Co->CoPerson->CoPersonRole->save($data, $args)) {
      $msg = "ERROR could not create CoPersonRole for CoPerson: ";
      $msg = $msg . "ACCESS ID $accessId ";
      $msg = $msg . "ERROR CoPersonRole validation errors: ";
      $msg = $msg . print_r($this->CoJob->Co->CoPerson->CoPersonRole->validationErrors, true);
      $this->log($msg);
      $dataSource->rollback();
      throw new RuntimeException($msg);
    }

    // Create an Org Identity.
    $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->clear();

    $data = array();
    $data['OrgIdentity'] = array();
    $data['OrgIdentity']['co_id'] = $this->coId;

    $args = array();
    $args['provision'] = false;

    if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->save($data, $args)) {
      $msg = "Could not create OrgIdentity for AccessID $accessId";
      $this->log($msg);
      $dataSource->rollback();
      throw new RuntimeException($msg);
    }

    // Record the OrgIdentity ID for later assignment.
    $orgIdentityId = $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->id;

    // Link the CoPerson to OrgIdentity.
    $this->CoJob->Co->CoPerson->CoOrgIdentityLink->clear();

    $data = array();
    $data['CoOrgIdentityLink'] = array();
    $data['CoOrgIdentityLink']['co_person_id'] = $coPersonId;
    $data['CoOrgIdentityLink']['org_identity_id'] = $orgIdentityId;

    $args = array();
    $args['provision'] = false;

    if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->save($data, $args)) {
      $msg = "ERROR could not link CoPerson and OrgIdentity ";
      $msg = $msg . "ACCESS ID $accessId ";
      $msg = $msg . "ERROR CoOrgIdentityLink validation errors: ";
      $msg = $msg . print_r($this->CoJob->Co->CoPerson->CoOrgIdentityLink->validationErrors, true);
      $this->log($msg);
      $dataSource->rollback();
      throw new RuntimeException($msg);
    }

    // Attach a Name to the OrgIdentity.
    $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->clear();

    $data = $this->nameArrayFromProfile($profile);

    $data['Name']['type'] = NameEnum::Official;
    $data['Name']['primary_name'] = true;
    $data['Name']['org_identity_id'] = $orgIdentityId;

    $args = array();
    $args['provision'] = false;

    if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->save($data, $args)) {
      $msg = "ERROR could not create Name for OrgIdentity: ";
      $msg = $msg . "ACCESS ID $accessId ";
      $msg = $msg . "ERROR Name validation errors: ";
      $msg = $msg .  print_r($this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->validationErrors, true);
      $this->log($msg);
      $dataSource->rollback();
      throw new RuntimeException($msg);
    }

    // Attach an Identifier of type EPPN to the OrgIdentity and
    // mark it as a login Identifier.
    $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Identifier->clear();

    $data = array();
    $data['Identifier'] = array();
    $data['Identifier']['identifier'] = $accessId . '@access-ci.org';
    $data['Identifier']['type'] = IdentifierEnum::ePPN;
    $data['Identifier']['status'] = SuspendableStatusEnum::Active;
    $data['Identifier']['login'] = true;
    $data['Identifier']['org_identity_id'] = $orgIdentityId;

    $args = array();
    $args['provision'] = false;

    if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Identifier->save($data, $args)) {
      $msg = "ERROR could not create Identifier for OrgIdentity: ";
      $msg = $msg . "ACCESS ID $accessId ";
      $msg = $msg . "ERROR Identifier validation errors: ";
      $msg = $msg . print_r($this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Identifier->validationErrors, true);
      $this->log($msg);
      $dataSource->rollback();
      throw new RuntimeException($msg);
    }

    // Commit the transaction.
    $dataSource->commit();

    // Force Identifier Assignment.
    $this->CoJob->Co->CoPerson->Identifier->assign('CoPerson', $coPersonId, null);

    // Now force manual provisioning.
    try {
      $this->CoJob->Co->CoPerson->manualProvision(null, $coPersonId, null, ProvisioningActionEnum::CoPersonAdded);
    } catch (Exception $e) {
      $msg = "Provisioning for CoPerson with ID $coPersonId threw exception: ";
      $msg = $msg . $e->getMessage();
      $this->log($msg);
      throw new RuntimeException($msg);
    }

    $msg = "Created CoPerson with ID $coPersonId for ACCESS ID $accessId";
    $this->log($msg);
  }

  /**
   * Execute the requested Job.
   *
   * @param  int   $coId    CO ID
   * @param  CoJob $CoJob   CO Job Object, id available at $CoJob->id
   * @param  array $params  Array of parameters, as requested via parameterFormat()
   * @throws InvalidArgumentException
   * @throws RuntimeException
   * @return void
   */
  public function execute($coId, $CoJob, $params) {
    $CoJob->update($CoJob->id, null, "full", null);

    $this->CoJob = $CoJob;
    $this->coId = $coId;

    if(array_key_exists('limit', $params)) {
      $this->pagingLimit = $params['limit'];
    }

    if(array_key_exists('offset', $params)) {
      $this->pagingOffset = $params['offset'];
    }

    if(array_key_exists('one_only', $params)) {
      $this->onePageOnly = $params['one_only'];
    }

    if(array_key_exists('access_id', $params)) {
      $this->requestedAccessId = $params['access_id'];
    }

    // Bootstrap if so configured.
    $bootstrap = Configure::read('AccessDbsyncJob.bootstrap');
    if($bootstrap) {
      // Create the ACCESS ID extended type if not present.
      $this->createAccessIdExtendedType();
    }

    if(!empty($this->requestedAccessId)) {
      $result = $this->synchronizeOneAccessId();
    } else {
      $result = $this->synchronizeAllProfiles();
    }

    $synchronized = $result['synchronized'];
    $failed = $result['failed'];
    $status = $result['status'];
    $summary = "Successfully synchronized $synchronized records and recorded $failed failures";

    $CoJob->finish($CoJob->id, $summary, $status);
  }

  /**
   * Find the CoPerson record linked to an Identifier of type accessid and
   * identifier equal to the input parameter. Return the CoPerson array with contained
   * CoPersonRole, EmailAddress, Identifier, Name, and CoOrgIdentityLink. The
   * CoOrgIdentityLink has OrgIdentity, Name, EmailAddress, and Identifier contained.
   * Return an empty array if no CoPerson is found.
   *
   * @param string $accessId ACCESS ID
   * @return array 
   */

  private function findCoPersonByAccessId($accessId) {

    // Search to see if this username exists.
    $args = array();
    $args['conditions']['Identifier.identifier'] = $accessId;
    $args['conditions']['Identifier.type'] = 'accessid';
    $args['contain'] = false;

    $identifier = $this->CoJob->Co->CoPerson->Identifier->find('first', $args);

    if(empty($identifier)) {
      return $identifier;
    } 

    $coPersonId = $identifier['Identifier']['co_person_id'];
    $args = array();
    $args['conditions']['CoPerson.id'] = $coPersonId;
    $args['contain'][] = 'CoPersonRole';
    $args['contain'][] = 'EmailAddress';
    $args['contain'][] = 'Identifier';
    $args['contain'][] = 'Name';
    $args['contain']['CoOrgIdentityLink']['OrgIdentity']['Name'] = [];
    $args['contain']['CoOrgIdentityLink']['OrgIdentity']['EmailAddress'] = [];
    $args['contain']['CoOrgIdentityLink']['OrgIdentity']['Identifier'] = [];

    $coPerson = $this->CoJob->Co->CoPerson->find('first', $args);

    return($coPerson);
  }

  /**
   * @since  COmanage Registry v4.0.0
   * @return Array Array of supported parameters.
   */

  public function getAvailableJobs() {
    $availableJobs = array();

    $availableJobs['AccessDbsync'] = "Synchronize with the ACCESS User Database";

    return $availableJobs;
  }

  /**
   * Create a Name array object from the User Database profile.
   * This method sets the given, middle, and family values for
   * the Name array using data from the profile. If given or family
   * is not available the string 'placeholder' is used. The middle
   * is only set if not empty in the profile. Other keys in the Name
   * array object are not set.
   *
   * @param  array $profile Profile returned from User Database
   * @return array Name array object
   */

  private function nameArrayFromProfile($profile) {
    $data = array();
    $data['Name'] = array();
    $data['Name']['given'] = (empty($profile['firstName'])) ? "placeholder" : $profile['firstName'];
    $data['Name']['family'] = (empty($profile['lastName'])) ? "placeholder" : $profile['lastName'];

    if(!empty($profile['middleName'])) {
      $data['Name']['middle'] = (empty($profile['middleName'])) ? "placeholder" : $profile['middleName'];
    }

    return $data;
  }

  /**
   * Obtain the list of parameters supported by this Job.
   *
   * @since  COmanage Registry v3.3.0
   * @return Array Array of supported parameters.
   */
  public function parameterFormat() {
    $params = array(
      'access_id' => array(
        'help' => _txt('pl.accessdbsyncjob.job.accessdbsync.access_id'),
        'type' => 'string',
        'required' => false
      ),
      'limit' => array(
        'help'     => _txt('pl.accessdbsyncjob.job.accessdbsync.limit'),
        'type'     => 'int',
        'required' => false
      ),
      'offset' => array(
        'help'     => _txt('pl.accessdbsyncjob.job.accessdbsync.offset'),
        'type'     => 'int',
        'required' => false 
      ),
      'one_only' => array(
        'help' => _txt('pl.accessdbsyncjob.job.accessdbsync.one_only'),
        'type' => 'bool',
        'required' => false
      )
    );

    return $params;
  }

  /** 
   * Page through the User Database and for each profile
   * synchronize the profile with an existing CO Person record
   * or create a CO Person record if one does not yet exist.
   *
   * @return array of number synchronized, number of failures, and JobStatusEnum
   */

  private function synchronizeAllProfiles() {
    $synchronized = 0;
    $failed = 0;
    $ret = array();

    // Page ACCESS User Database profiles and continue until done.
    $pagingDone = false;

    if(empty($this->pagingOffset)) {
      $offset = Configure::read('AccessDbsyncJob.db.profile.page.initial.offset');
    } else {
      $offset = $this->pagingOffset;
    }

    if(empty($this->pagingLimit)) {
      $limit = Configure::read('AccessDbsyncJob.db.profile.page.limit');
    } else {
      $limit = $this->pagingLimit;
    }

    $maxSleepTime = Configure::read('AccessDbsyncJob.db.profile.page.max.sleep.time.seconds');
    $sleepTime = 1;

    while(!$pagingDone) {
      // Return if this CoJob invocation has been cancelled.
      if($this->CoJob->canceled($this->CoJob->id)) {
        $ret['synchronized'] = $synchronized;
        $ret['failed'] = $failed;
        $ret['status'] = JobStatusEnum::Canceled;
        return $ret;
      }

      // We record a job history record for each page with the page
      // indexed by the combination of offset and limit.
      $jobHistoryRecordKey = "offset=$offset" . '&' . "limit=$limit";

      // Configure curl libraries to query ACCESS Database API.
      $urlBase = Configure::read('AccessDbsyncJob.db.profile.page.url.base');
      $url = $urlBase . '/people?' . "limit=$limit" . '&' . "offset=$offset";
      $ch = curl_init($url);
      curl_setopt($ch, CURLOPT_URL, $url);

      // Include headers necessary for authentication.
      $headers = array();
      $headers[] = 'XA-REQUESTER: ' . Configure::read('AccessDbsyncJob.db.api.requester');
      $headers[] = 'XA-API-KEY: ' .  Configure::read('AccessDbsyncJob.db.api.key');
      curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);

      // Return the payload from the curl_exec call below.
      curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

      // Make the query and get the response and return code.
      $response = curl_exec($ch);
      $curlReturnCode = curl_getinfo($ch, CURLINFO_RESPONSE_CODE);

      curl_close($ch);

      if($response === false) {
        $jobHistoryComment = "Unable to query ACCESS User Database profile page endpoint";
        $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, null, null, JobStatusEnum::Failed);
        $this->log($jobHistoryComment);
        $failed++;

        // Sleep and try again.
        $this->log("Sleeping for $sleepTime seconds...");
        sleep($sleepTime);
        $this->log("Awake from sleep");

        $sleepTime = $sleepTime * 2;
        if($sleepTime > $maxSleepTime) {
          $sleepTime = $maxSleepTime;
        }
        continue;
      }

      if($curlReturnCode != 200) {
        $jobHistoryComment = "Query to ACCESS User Database profile endpoint returned code $curlReturnCode";
        $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, null, null, JobStatusEnum::Failed);
        $this->log($jobHistoryComment);
        $failed++;

        // Sleep and try again.
        $this->log("Sleeping for $sleepTime seconds...");
        sleep($sleepTime);
        $this->log("Awake from sleep");

        $sleepTime = $sleepTime * 2;
        if($sleepTime > $maxSleepTime) {
          $sleepTime = $maxSleepTime;
        }
        continue;
      }

      try {
        $profileList = json_decode($response, true, 512, JSON_THROW_ON_ERROR);
      } catch (Exception $e) {
        $jobHistoryComment = "Error decoding JSON from User Database: " . $e->getMessage();
        $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, null, null, JobStatusEnum::Failed);
        $this->log($jobHistoryComment);
        $failed++;

        // Sleep but go onto the next page.
        $this->log("Sleeping for $sleepTime seconds...");
        sleep($sleepTime);
        $this->log("Awake from sleep");

        $sleepTime = $sleepTime * 2;
        if($sleepTime > $maxSleepTime) {
          $sleepTime = $maxSleepTime;
        }
      }

      // Loop over the list of returned profiles and synchronize each one.
      if(count($profileList) > 0) {
        $pageSynchronized = 0;
        $pageFailed = 0;

        foreach ($profileList as $profile) {
          try {
            $this->synchronizeOneProfile($profile);
            $pageSynchronized += 1;
          } catch (Exception $e) {
            $pageFailed += 1;

            // Record a job history record for each individual failure.
            if(!empty($profile['username'])) {
              $key = "ACCESS ID " . $profile['username'];
            } else {
              $key = "Unknown ACCESS ID";
            }
            $comment = $e->getMessage();
            $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $key, $comment, null, null, JobStatusEnum::Failed);
          }
        }

        $jobHistoryComment = "Synchronized $limit users starting with offset $offset, recorded $pageSynchronized successes and $pageFailed failures";
        $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, null, null, JobStatusEnum::Complete);

        $synchronized += $pageSynchronized;
        $failed += $pageFailed;

        // If at least one profile was successfully synchronized
        // reset the sleepTime.
        if($pageSynchronized > 0) {
          $sleepTime = 1;
        }

        // Prepare to query for and process the next page.
        $offset += $limit;
        
        // Flag used for development and testing.
        if($this->onePageOnly === null) {
          $onePageOnly = Configure::read('AccessDbsyncJob.debug.one.page.only');
        } else {
          $onePageOnly = $this->onePageOnly;
        }

        if($onePageOnly) {
          $pagingDone = true;
        } 
      } else {
        $pagingDone = true;
      }
    }

    $ret['synchronized'] = $synchronized;
    $ret['failed'] = $failed;
    $ret['status'] = JobStatusEnum::Complete;

    return $ret;
  }

  /** 
   * Synchronize the profile for one ACCESS ID with an existing 
   * CO Person record or create a CO Person record if one does
   * not yet exist.
   *
   * @return array of number synchronized, number of failures, and JobStatusEnum
   */

  private function synchronizeOneAccessId() {
    $ret['synchronized'] = 0;
    $ret['failed'] = 1;
    $ret['status'] = JobStatusEnum::Failed;

    $requestedAccessId = $this->requestedAccessId;

    // We record a job history record based on the ACCESS ID.
    $jobHistoryRecordKey = $requestedAccessId;

    // Configure curl libraries to query ACCESS Database API.
    $urlBase = Configure::read('AccessDbsyncJob.db.profile.page.url.base');
    $url = $urlBase . '/people/' . $requestedAccessId;
    $ch = curl_init($url);
    curl_setopt($ch, CURLOPT_URL, $url);

    // Include headers necessary for authentication.
    $headers = array();
    $headers[] = 'XA-REQUESTER: ' . Configure::read('AccessDbsyncJob.db.api.requester');
    $headers[] = 'XA-API-KEY: ' .  Configure::read('AccessDbsyncJob.db.api.key');
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);

    // Return the payload from the curl_exec call below.
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

    // Make the query and get the response and return code.
    $response = curl_exec($ch);
    $curlReturnCode = curl_getinfo($ch, CURLINFO_RESPONSE_CODE);

    curl_close($ch);

    if($response === false) {
      $jobHistoryComment = "Unable to query ACCESS User Database profile page endpoint";
      $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, null, null, JobStatusEnum::Failed);
      $this->log($jobHistoryComment);
      return $ret;
    }

    if($curlReturnCode != 200) {
      $jobHistoryComment = "Query to ACCESS User Database profile endpoint returned code $curlReturnCode";
      $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, null, null, JobStatusEnum::Failed);
      $this->log($jobHistoryComment);
      return $ret;
    }

    try {
      $profile = json_decode($response, true, 512, JSON_THROW_ON_ERROR);
    } catch (Exception $e) {
      $jobHistoryComment = "Error decoding JSON from User Database: " . $e->getMessage();
      $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, null, null, JobStatusEnum::Failed);
      $this->log($jobHistoryComment);
      return $ret;
    }

    try {
      $this->synchronizeOneProfile($profile);
    } catch (Exception $e) {
      $jobHistoryComment = "Error synchronizing profile for ACCESS ID $requestedAccessId: " . $e->getMessage();
      $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, null, null, JobStatusEnum::Failed);
      $this->log($jobHistoryComment);
      return $ret;
    }

    $jobHistoryComment = "Synchronized profile for ACCESS ID $requestedAccessId";
    $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, null, null, JobStatusEnum::Complete);

    $ret['synchronized'] = 1;
    $ret['failed'] = 0;
    $ret['status'] = JobStatusEnum::Complete;

    return $ret;
  }

  /**
   * Synchronize a profile from the User Database with an existing
   * CO Person record or create a new CO Person record.
   *
   * @param  array $profile Profile returned from User Database
   * @return void
   * @throws RuntimeException
   */

  private function synchronizeOneProfile($profile) {
    $accessId = $this->accessIdFromProfile($profile);

    // Skip the unknown user profile.
    if($accessId === "unknown-user") {
      return;
    }

    // Find an existing CoPerson record for this ACCESS ID or
    // an empty array if one does not exist.
    $coPerson = $this->findCoPersonByAccessId($accessId);

    if(empty($coPerson)) {
      $this->createCoPersonFromProfile($profile);
    } else {
      $this->synchronizeCoPerson($coPerson, $profile);
    }
  }

  /*
   * Synchronize an existing CoPerson record with the corresponding
   * profile from the User Database.
   *
   * @param array $coPerson CoPerson array object with contained objects
   * @param array $profile Profile returned from User Database
   * @return void
   * @throws RuntimeException
   */

  private function synchronizeCoPerson($coPerson, $profile) {
    $coPersonId = $coPerson['CoPerson']['id'];

    // Find the existing CO Person primary name.
    $primaryName = null;
    foreach($coPerson['Name'] as $name) {
      if($name['primary_name']) {
        $primaryName = $name;
      }
    }

    // Create a Name array object from profile.
    $profileName = $this->nameArrayFromProfile($profile);
    $profileName['Name']['type'] = NameEnum::Official;
    $profileName['Name']['primary_name'] = true;
    $profileName['Name']['co_person_id'] = $coPersonId;
    
    // Flag to indicate if the primary name needs to be updated
    // using the profile.
    $updateName = false;

    // Compare the profile name and CO Person PrimaryName.
    if(!empty($primaryName)) {

      if($primaryName['given'] != $profileName['Name']['given']) {
        $updateName = true;
      }

      if($primaryName['family'] != $profileName['Name']['family']) {
        $updateName = true;
      }

      if(!empty($primaryName['middle']) && empty($profileName['Name']['middle'])) {
        $updateName = true;
      }

      if(empty($primaryName['middle']) && !empty($profileName['Name']['middle'])) {
        $updateName = true;
      }

      if(!empty($primaryName['middle']) && !empty($profileName['Name']['middle'])) {
        if($primaryName['middle'] != $profileName['Name']['middle']) {
          $updateName = true;
        }
      }
    } 
    
    if(empty($primaryName) || $updateName) {
      $this->CoJob->Co->CoPerson->Name->clear();

      // Set the ID if this is an update.
      if($updateName) {
        $profileName['Name']['id'] = $primaryName['id'];
      }

      if(!$this->CoJob->Co->CoPerson->Name->save($profileName)) {
        $msg = "ERROR could not update Name for CoPerson: ";
        $msg = $msg . "ERROR Name validation errors: ";
        $msg = $msg . print_r($this->CoJob->Co->CoPerson->Name->validationErrors, true);
        $this->log($msg);
        throw new RuntimeException($msg);
      }
    }
    
    // Compare the profile email address and EmailAddress of type Official.
    // Note there may be more than one EmailAddress of type Official.
    // If we do not find an EmailAddress of type Official with the same value
    // as in profile then we create a new one since we cannot be sure which one
    // to overwrite or if overwriting is the correct action since the user may
    // have updated the email and synchronization failed with the central database.
    $synchronized = false;
    foreach($coPerson['EmailAddress'] as $email) {
      if(($email['mail'] == $profile['email']) &&
        ($email['type'] == EmailAddressEnum::Official)) {
        $synchronized = true;
      }
    }

    if(!$synchronized) {
      $this->CoJob->Co->CoPerson->EmailAddress->clear();

      $data = array();
      $data['EmailAddress'] = array();
      $data['EmailAddress']['mail'] = (empty($profile['email'])) ? "placeholder" : $profile['email'];
      $data['EmailAddress']['description'] = 'Synched from central User Database';
      $data['EmailAddress']['type'] = EmailAddressEnum::Official;
      $data['EmailAddress']['verified'] = true;
      $data['EmailAddress']['co_person_id'] = $coPersonId;

      $options = array();
      $options['trustVerified'] = true;

      if(!$this->CoJob->Co->CoPerson->EmailAddress->save($data, $options)) {
        $msg = "ERROR could not create EmailAddress for CoPerson $coPersonId: ";
        $msg = $msg . "ERROR EmailAddress validation errors: ";
        $msg = $msg . print_r($this->CoJob->Co->CoPerson->EmailAddress->validationErrors, true);
        $this->log($msg);
        throw new RuntimeException($msg);
      }
    }

    // Loop over CO Person Roles and verify there is at least one with affiliation Affiliate
    // and Organization as determined by the profile. 
    $synchronized = false;
    foreach($coPerson['CoPersonRole'] as $role) {
      if(empty($profile['organizationName'])) {
          $profileOrg = "placeholder";
      } else {
          $profileOrg = $profile['organizationName'];
      }

      // Before comparing the organization saved as a field for the CoPersonRole
      // with the value from the ACCESS DB profile, we need to perform the same
      // normalization on the value from the ACCESS DB profile that the COmanage Registry
      // DefaultNormalizer plugin does.

      $profileOrgNormalized = mb_convert_case($profileOrg, MB_CASE_TITLE);

      if(($role['affiliation'] == AffiliationEnum::Affiliate) &&
         (($role['o'] == $profileOrg) || ($role['o'] == $profileOrgNormalized))) {
        $synchronized = true;
        break;
      }
    }

    if(!$synchronized) {
      $this->CoJob->Co->CoPerson->CoPersonRole->clear();
      
      $data = array();
      $data['CoPersonRole'] = array();
      $data['CoPersonRole']['co_person_id'] = $coPersonId;
      $data['CoPersonRole']['status'] = StatusEnum::Active;
      $data['CoPersonRole']['affiliation'] = AffiliationEnum::Affiliate;
      $data['CoPersonRole']['o'] = (empty($profile['organizationName'])) ? "placeholder" : $profile['organizationName'];

      if(!$this->CoJob->Co->CoPerson->CoPersonRole->save($data)) {
        $msg = "ERROR could not create CoPersonRole for CoPerson: ";
        $msg = $msg . "ERROR CoPersonRole validation errors: ";
        $msg = $msg . print_r($this->CoJob->Co->CoPerson->CoPersonRole->validationErrors, true);
        $this->log($msg);
        throw new RuntimeException($msg);
      }
    }

    // Loop over OrgIds and verify there is at least one with the ACCESS ID as
    // the ePPN with login privilege, or if not create a new OrgId.
    $synchronized = false;
    $accessId = $this->accessIdFromProfile($profile);
    $desiredEppn = $accessId . '@access-ci.org';
    foreach($coPerson['CoOrgIdentityLink'] as $link) {
      if(!empty($link['OrgIdentity']['Identifier'])) {
        foreach($link['OrgIdentity']['Identifier'] as $identifier) {
          if(($identifier['identifier'] == $desiredEppn) &&
             ($identifier['type'] == IdentifierEnum::ePPN)) {
            $synchronized = true;
            $synchronizedOrgIdentity = $link['OrgIdentity'];
          }
        }
      }
    }

    if(!$synchronized) {
      // Create a new OrgId.
      
      // Begin a transaction.
      $dataSource = $this->CoJob->getDataSource();
      $dataSource->begin();

      // Create a new OrgIdentity using the profile.
      $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->clear();

      $data = array();
      $data['OrgIdentity'] = array();
      $data['OrgIdentity']['co_id'] = $this->coId;

      if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->save($data)) {
        $msg = "Could not create OrgIdentity for CoPerson $coPersonId";
        $this->log($msg);
        $dataSource->rollback();
        throw new RuntimeException($msg);
      }

      // Record the OrgIdentity ID for later assignment.
      $orgIdentityId = $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->id;

      // Link the CoPerson to OrgIdentity.
      $this->CoJob->Co->CoPerson->CoOrgIdentityLink->clear();

      $data = array();
      $data['CoOrgIdentityLink'] = array();
      $data['CoOrgIdentityLink']['co_person_id'] = $coPersonId;
      $data['CoOrgIdentityLink']['org_identity_id'] = $orgIdentityId;

      if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->save($data)) {
        $msg = "ERROR could not link CoPerson and OrgIdentity ";
        $msg = $msg . "ERROR CoOrgIdentityLink validation errors: ";
        $msg = $msg . print_r($this->CoJob->Co->CoPerson->CoOrgIdentityLink->validationErrors, true);
        $this->log($msg);
        $dataSource->rollback();
        throw new RuntimeException($msg);
      }

      // Attach a Name to the OrgIdentity.
      $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->clear();

      $data = $this->nameArrayFromProfile($profile);

      $data['Name']['type'] = NameEnum::Official;
      $data['Name']['primary_name'] = true;
      $data['Name']['org_identity_id'] = $orgIdentityId;

      if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->save($data)) {
        $msg = "ERROR could not create Name for OrgIdentity: ";
        $msg = $msg . "ERROR Name validation errors: ";
        $msg = $msg .  print_r($this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->validationErrors, true);
        $this->log($msg);
        $dataSource->rollback();
        throw new RuntimeException($msg);
      }

      // Attach an Identifier of type EPPN to the OrgIdentity and
      // mark it as a login Identifier.
      $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Identifier->clear();

      $data = array();
      $data['Identifier'] = array();
      $data['Identifier']['identifier'] = $accessId . '@access-ci.org';
      $data['Identifier']['type'] = IdentifierEnum::ePPN;
      $data['Identifier']['status'] = SuspendableStatusEnum::Active;
      $data['Identifier']['login'] = true;
      $data['Identifier']['org_identity_id'] = $orgIdentityId;

      if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Identifier->save($data)) {
        $msg = "ERROR could not create Identifier for OrgIdentity: ";
        $msg = $msg . "ERROR Identifier validation errors: ";
        $msg = $msg . print_r($this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Identifier->validationErrors, true);
        $this->log($msg);
        $dataSource->rollback();
        throw new RuntimeException($msg);
      }

      // Commit the transaction.
      $dataSource->commit();
    } else {
      // Make sure the Name attached to the OrgIdentity is also synchronized.

      // Find the existing OrgId primary name.
      $primaryName = null;
      foreach($synchronizedOrgIdentity['Name'] as $name) {
        if($name['primary_name']) {
          $primaryName = $name;
        }
      }

      // Create a Name array object from profile.
      $profileName = $this->nameArrayFromProfile($profile);
      $profileName['Name']['type'] = NameEnum::Official;
      $profileName['Name']['primary_name'] = true;
      $profileName['Name']['org_person_id'] = $synchronizedOrgIdentity['id'];
      
      // Flag to indicate if the primary name needs to be updated
      // using the profile.
      $updateName = false;

      // Compare the profile name and CO Person PrimaryName.
      if(!empty($primaryName)) {

        if($primaryName['given'] != $profileName['Name']['given']) {
          $updateName = true;
        }

        if($primaryName['family'] != $profileName['Name']['family']) {
          $updateName = true;
        }

        if(!empty($primaryName['middle']) && empty($profileName['Name']['middle'])) {
          $updateName = true;
        }

        if(empty($primaryName['middle']) && !empty($profileName['Name']['middle'])) {
          $updateName = true;
        }

        if(!empty($primaryName['middle']) && !empty($profileName['Name']['middle'])) {
          if($primaryName['middle'] != $profileName['Name']['middle']) {
            $updateName = true;
          }
        }
      } 
      
      if(empty($primaryName) || $updateName) {
        $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->clear();

        // Set the ID if this is an update.
        if($updateName) {
          $profileName['Name']['id'] = $primaryName['id'];
        }

        if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->save($profileName)) {
          $msg = "ERROR could not update Name for CoPerson: ";
          $msg = $msg . "ERROR Name validation errors: ";
          $msg = $msg . print_r($this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->validationErrors, true);
          $this->log($msg);
          throw new RuntimeException($msg);
        }
      }
    }
  }
}
