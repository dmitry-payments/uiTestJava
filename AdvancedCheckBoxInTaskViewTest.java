package com.infa.cloud.uitests.dbmi.tasks;

import com.infa.cloud.apitests.service.APIService;
import com.infa.cloud.common.annotation.Cleanup;
import com.infa.cloud.common.annotation.Report;
import com.infa.cloud.common.domain.Connection;
import com.infa.cloud.common.domain.Task;
import com.infa.cloud.common.logger.ExtLogger;
import com.infa.cloud.jira.annotation.Jira;
import com.infa.cloud.jira.annotation.JiraTestCaseId;
import com.infa.cloud.uitests.core.BaseUITest;
import com.infa.cloud.uitests.domain.DataIngestion;
import com.infa.cloud.uitests.pageobjects.datainjection.explore.DataIngestionExplore;
import com.infa.cloud.uitests.pageobjects.datainjection.explore.DataIngestionTaskView;
import com.infa.cloud.uitests.pageobjects.datainjection.explore.enums.view.TaskLabels;
import com.infa.cloud.uitests.pageobjects.datainjection.wizard.DataIngestionWizardStep1;
import com.infa.cloud.uitests.pageobjects.datainjection.wizard.DataIngestionWizardStep3;
import com.infa.cloud.uitests.pageobjects.datainjection.wizard.DataIngestionWizardStep4;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

/**
 * DBMI-952 Verification of display 'Advanced' checkbox' in Task View.(UNLOAD Oracle->Flat)
 * DBMI-5162 Verification of display 'Advanced' checkbox in Task View.(UNLOAD Oracle->S3)
 * DBMI-5163 Verification of display 'Advanced' checkbox in Task View.(UNLOAD Oracle->adlsgen2)
 * DBMI-5164 Verification of display 'Advanced' checkbox in Task View.(CDC Oracle->S3)
 * DBMI-5165 Verification of display 'Advanced' checkbox in Task View.(CDC Oracle->adlsgen2)
 * DBMI-5166 Verification of display 'Advanced' checkbox in Task View.(CDC Oracle->Kafka)
 * DBMI-8409 Verification of display 'Advanced' checkbox in Task View.(CDC Oracle->Google)
 * @author eveselov
 */
@Jira
public class AdvancedCheckBoxInTaskViewTest extends BaseUITest {

    private static ExtLogger LOG = ExtLogger.create(AdvancedCheckBoxInTaskViewTest.class);

    DataIngestionTaskView taskView;
    DataIngestionExplore explore;
    DataIngestionWizardStep1 step1;
    DataIngestionWizardStep3 step3;

    @Cleanup
    public Task task1, task2, task3;
    String targetConnection;

    @Report
    String currentDataSet;

    @JiraTestCaseId
    String jiraTestCaseId;

    @DataProvider(name = "tasks")
    public static Object[][] getData() {
        return readParametersFromYaml();
    }

    @Factory(dataProvider = "tasks")
    public AdvancedCheckBoxInTaskViewTest(String jiraTestCaseId, String loadType, String sourceConnection, String sourceSchemaText, String targetConnection,
                                          String targetSchemaText, String location) {
        this.jiraTestCaseId = jiraTestCaseId;
        task1 = new Task(jiraTestCaseId + "_", loadType, sourceConnection, sourceSchemaText, targetConnection, targetSchemaText);
        task1.setLocation(location);

        task2 = task1.clone();
        task3 = task2.clone();

        this.targetConnection = targetConnection;
        currentDataSet = String.format("Source: %s, Target: %s", sourceConnection, targetConnection);
    }

    @Test
    public void verifyAdvancedCheckBoxesInView() {
        SoftAssert softAssert = new SoftAssert();
        LOG.step(currentDataSet);
        LOG.step("Preconditions: Created Tasks Task1, Task2, Task3 without Advanced Check Box in Target.");
        APIService.createTask(task1.setUniqueName());
        APIService.createTask(task2.setUniqueName());
        APIService.createTask(task3.setUniqueName());

        LOG.step("1. Go to 'Explore' -> Project or Folder with saved Task1 -> Add 'Advanced Check' Box in Target -> Save the task.");
        step1 = DataIngestion.openTask(task1);
        addAdvancedCheckBox(task1);

        LOG.step("2. After saving task click 'View' button'. ");
        taskView = step3.view();

        LOG.step("3.'View' page -> Check the value of 'Advanced' checkbox ('Add Operation Type', 'Add Operation Time', 'Add Operation Owner', 'Add Operation Transaction Id' and 'CDC compatible Format'). ");
        String actualOperationTypeValue = taskView.getTargetValue(TaskLabels.ADD_OP_TYPE);
        String actualOperationTimeValue = taskView.getTargetValue(TaskLabels.ADD_OP_TIME);
        String actualOperationOwnerValue = taskView.getTargetValue(TaskLabels.ADD_OP_OWNER);
        String actualOperationTransactionIdValue = taskView.getTargetValue(TaskLabels.ADD_OP_TXID);
        String actualAddBeforeImagesValue = taskView.getTargetValue(TaskLabels.ADD_BEFORE_IMAGES);

        LOG.step("Expected result: 'Advanced' checkbox ('Add Operation Type', 'Add Operation Time', 'Add Operation Owner', 'Add Operation Transaction Id' and 'CDC compatible Format') is in Target and the value is true. ");
        softAssert.assertEquals(actualOperationTypeValue, task1.isOperationTypeStr(), String.format("Wrong Add Operation Type value when Task1 %s is opened from 'View' ", task1.getName()));
        softAssert.assertEquals(actualOperationTimeValue, task1.isOperationTimeStr(), String.format("Wrong Add Operation Time value when Task1 %s is opened from 'View' ", task1.getName()));
        softAssert.assertEquals(actualOperationOwnerValue, task1.isOperationOwnerStr(), String.format("Wrong Add Operation Owner value when Task1 %s is opened from 'View' ", task1.getName()));
        softAssert.assertEquals(actualOperationTransactionIdValue, task1.isOperationTransactionIdStr(), String.format("Wrong Add Operation Transaction Id value when Task1 %s is opened from 'View' ", task1.getName()));
        softAssert.assertEquals(actualAddBeforeImagesValue, task1.isAddBeforeImagesStr(), String.format("Wrong Add before Images value when Task1 %s is opened from 'View' for Task1. ", task1.getName()));

        LOG.step("3.1 Check the value of 'Use Cycle Partitioning for Data Directory', 'Use Cycle Partitioning for Summary Directories', 'List Individual Files in Contents'");
        if (task1.isCDC() && new Connection(targetConnection).isFileStorage()) {
            softAssert.assertEquals(taskView.getTargetValue(TaskLabels.USE_CYCLE_PARTITIONING_FOR_DATA_DIRECTORY), task1.isUseCyclePartitioningForDataDirectoryStr(), String.format("Wrong Use Cycle Partitioning for Data Directory value when Task1 %s is opened from 'View' for Task1. ", task1.getName()));
            softAssert.assertEquals(taskView.getTargetValue(TaskLabels.USE_CYCLE_PARTITIONING_FOR_SUMMARY_DIRECTORIES), task1.isUseCyclePartitioningForSummaryDirectoriesStr(), String.format("Wrong Use Cycle Partitioning for Summary Directories value when Task1 %s is opened from 'View' for Task1. ", task1.getName()));
            softAssert.assertEquals(taskView.getTargetValue(TaskLabels.LIST_INDIVIDUAL_FILES_IN_CONTENTS), task1.isListIndividualFilesInContentsStr(), String.format("Wrong List Individual Files in Contents value when Task1 %s is opened from 'View' for Task1. ", task1.getName()));
            LOG.step("3.2 Change the value of 'Use Cycle Partitioning for Data Directory' to false, then check that 'List Individual Files in Contents' is true");
            taskView = changeAdditionalCheckboxesForTask1(false, task1);
            softAssert.assertTrue(Boolean.parseBoolean(taskView.getTargetValue(TaskLabels.LIST_INDIVIDUAL_FILES_IN_CONTENTS)), String.format("Wrong List Individual Files in Contents value when 'Use Cycle Partitioning for Data Directory' is false. ", task1.getName()));
            LOG.step("3.3 Change the value of 'Use Cycle Partitioning for Data Directory' to true, then check 'List Individual Files in Contents' is false");
            taskView = changeAdditionalCheckboxesForTask1(true, task1);
            softAssert.assertFalse(Boolean.parseBoolean(taskView.getTargetValue(TaskLabels.LIST_INDIVIDUAL_FILES_IN_CONTENTS)), String.format("Wrong List Individual Files in Contents value when 'Use Cycle Partitioning for Data Directory' is false. ", task1.getName()));
        }

        LOG.step("3.4 Close the 'View' page. ");
        taskView.close();

        LOG.step("4. Go to 'Explore' -> Project or Folder with saved Task2 -> Add 'Advanced Check' Box in Target -> Save the task.");
        explore = step3.mainMenu.openExplore();
        explore.getExploreTable().deselectRow(task1.getName());
        explore.openTask(task2.getName());
        step3 = addAdvancedCheckBox(task2);

        LOG.step("5. Back to 'Explore' -> Choose the 'View' of task from 'Selected' container. ");
        explore = step3.mainMenu.openExplore();
        taskView = explore.viewTaskFromSelectedContainer(task2.getName());

        LOG.step("6.'View' page -> Check the value of 'Advanced' checkbox ('Add Operation Type', 'Add Operation Time', 'Add Operation Owner', 'Add Operation Transaction Id' and 'CDC compatible Format'). ");
        actualOperationTypeValue = taskView.getTargetValue(TaskLabels.ADD_OP_TYPE);
        actualOperationTimeValue = taskView.getTargetValue(TaskLabels.ADD_OP_TIME);
        actualOperationOwnerValue = taskView.getTargetValue(TaskLabels.ADD_OP_OWNER);
        actualOperationTransactionIdValue = taskView.getTargetValue(TaskLabels.ADD_OP_TXID);
        actualAddBeforeImagesValue = taskView.getTargetValue(TaskLabels.ADD_BEFORE_IMAGES);

        LOG.step("Expected result: 'Advanced' checkbox ('Add Operation Type', 'Add Operation Time', 'Add Operation Owner', 'Add Operation Transaction Id' and 'CDC compatible Format') is in Target and the value is true.");
        softAssert.assertEquals(actualOperationTypeValue, task2.isOperationTypeStr(), String.format("Wrong Add Operation Type value when Task2 %s is opened from selected container ", task2.getName()));
        softAssert.assertEquals(actualOperationTimeValue, task2.isOperationTimeStr(), String.format("Wrong Add Operation Time value when Task2 %s is opened from selected container ", task2.getName()));
        softAssert.assertEquals(actualOperationOwnerValue, task2.isOperationOwnerStr(), String.format("Wrong Add Operation Owner value when Task2 %s is opened from selected container ", task2.getName()));
        softAssert.assertEquals(actualOperationTransactionIdValue, task2.isOperationTransactionIdStr(), String.format("Wrong Add Operation Transaction Id value when Task2 %s is opened from selected container ", task2.getName()));
        softAssert.assertEquals(actualAddBeforeImagesValue, task2.isAddBeforeImagesStr(), String.format("Wrong Add before Images value when Task2 %s is opened from selected container " , task2.getName()));

        LOG.step("6.1 Check the value of 'Use Cycle Partitioning for Data Directory', 'Use Cycle Partitioning for Summary Directories', 'List Individual Files in Contents'");
        if (task2.isCDC() && new Connection(targetConnection).isFileStorage()) {
            softAssert.assertEquals(taskView.getTargetValue(TaskLabels.USE_CYCLE_PARTITIONING_FOR_DATA_DIRECTORY), task2.isUseCyclePartitioningForDataDirectoryStr(), String.format("Wrong Use Cycle Partitioning for Data Directory value when Task1 %s is opened from 'View' for Task1. ", task2.getName()));
            softAssert.assertEquals(taskView.getTargetValue(TaskLabels.USE_CYCLE_PARTITIONING_FOR_SUMMARY_DIRECTORIES), task2.isUseCyclePartitioningForSummaryDirectoriesStr(), String.format("Wrong Use Cycle Partitioning for Summary Directories value when Task1 %s is opened from 'View' for Task1. ", task2.getName()));
            softAssert.assertEquals(taskView.getTargetValue(TaskLabels.LIST_INDIVIDUAL_FILES_IN_CONTENTS), task2.isListIndividualFilesInContentsStr(), String.format("Wrong List Individual Files in Contents value when Task1 %s is opened from 'View' for Task1. ", task2.getName()));
            LOG.step("6.2 Change the value of 'Use Cycle Partitioning for Summary Directories' to false and 'List Individual Files in Contents' to true, then check that 'Use Cycle Partitioning for Data Directory' is true");
            taskView = changeAdditionalCheckboxesForTask2(false, true, task2);
            softAssert.assertTrue(Boolean.parseBoolean(taskView.getTargetValue(TaskLabels.USE_CYCLE_PARTITIONING_FOR_DATA_DIRECTORY)), String.format("Wrong Use Cycle Partitioning for Data Directory value. ", task2.getName()));
            LOG.step("6.3 Change the value of 'Use Cycle Partitioning for Summary Directories' to true and 'List Individual Files in Contents' to false, then check that 'Use Cycle Partitioning for Data Directory' is true");
            taskView = changeAdditionalCheckboxesForTask2(true, false, task2);
            softAssert.assertTrue(Boolean.parseBoolean(taskView.getTargetValue(TaskLabels.USE_CYCLE_PARTITIONING_FOR_DATA_DIRECTORY)), String.format("Wrong Use Cycle Partitioning for Data Directory value. ", task2.getName()));
        }

        LOG.step("6.4 Close the 'View' page. ");
        taskView.close();

        LOG.step("7. Go to 'Explore' -> Project or Folder with saved Task3 -> Add 'Advanced Check' Box in Target -> Save. ");
        explore = step3.mainMenu.openExplore();
        explore.getExploreTable().deselectRow(task2.getName());
        explore.openTask(task3.getName());
        step3 = addAdvancedCheckBox(task3);

        LOG.step("8. Back to 'Explore' -> Choose the 'View' of task from floating toolbar. ");
        explore = step3.mainMenu.openExplore();
        taskView = explore.viewTaskFromFloatingToolbar(task3.getName());

        LOG.step("9.'View' page -> Check the value of 'Advanced' checkbox ('Add Operation Type', 'Add Operation Time', 'Add Operation Owner', 'Add Operation Transaction Id' and 'CDC compatible Format'). ");
        actualOperationTypeValue = taskView.getTargetValue(TaskLabels.ADD_OP_TYPE);
        actualOperationTimeValue = taskView.getTargetValue(TaskLabels.ADD_OP_TIME);
        actualOperationOwnerValue = taskView.getTargetValue(TaskLabels.ADD_OP_OWNER);
        actualOperationTransactionIdValue = taskView.getTargetValue(TaskLabels.ADD_OP_TXID);
        actualAddBeforeImagesValue = taskView.getTargetValue(TaskLabels.ADD_BEFORE_IMAGES);

        LOG.step("Expected result: 'Advanced' checkbox ('Add Operation Type', 'Add Operation Time', 'Add Operation Owner', 'Add Operation Transaction Id' and 'CDC compatible Format') is in Target and the value is true. ");
        softAssert.assertEquals(actualOperationTypeValue, task3.isOperationTypeStr(), String.format("Wrong Add Operation Type value when Task3 %s is opened from floating toolbar ", task3.getName()));
        softAssert.assertEquals(actualOperationTimeValue, task3.isOperationTimeStr(), String.format("Wrong Add Operation Time value when Task3 %s is opened from floating toolbar ", task3.getName()));
        softAssert.assertEquals(actualOperationOwnerValue, task3.isOperationOwnerStr(), String.format("Wrong Add Operation Owner value when Task3 %s is opened from floating toolbar ", task3.getName()));
        softAssert.assertEquals(actualOperationTransactionIdValue, task3.isOperationTransactionIdStr(), String.format("Wrong Add Operation Transaction Id value when Task3 %s is opened from floating toolbar ", task3.getName()));
        softAssert.assertEquals(actualAddBeforeImagesValue, task3.isAddBeforeImagesStr(), String.format("Wrong Add before Images value when Task3 %s is opened from floating toolbar ", task3.getName()));

        LOG.step("9.1 Check the value of 'Use Cycle Partitioning for Data Directory', 'Use Cycle Partitioning for Summary Directories', 'List Individual Files in Contents'");
        if (task3.isCDC() && new Connection(targetConnection).isFileStorage()) {
            softAssert.assertEquals(taskView.getTargetValue(TaskLabels.USE_CYCLE_PARTITIONING_FOR_DATA_DIRECTORY), task3.isUseCyclePartitioningForDataDirectoryStr(), String.format("Wrong Use Cycle Partitioning for Data Directory value when Task1 %s is opened from 'View' for Task1. ", task3.getName()));
            softAssert.assertEquals(taskView.getTargetValue(TaskLabels.USE_CYCLE_PARTITIONING_FOR_SUMMARY_DIRECTORIES), task3.isUseCyclePartitioningForSummaryDirectoriesStr(), String.format("Wrong Use Cycle Partitioning for Summary Directories value when Task1 %s is opened from 'View' for Task1. ", task3.getName()));
            softAssert.assertEquals(taskView.getTargetValue(TaskLabels.LIST_INDIVIDUAL_FILES_IN_CONTENTS), task3.isListIndividualFilesInContentsStr(), String.format("Wrong List Individual Files in Contents value when Task1 %s is opened from 'View' for Task1. ", task3.getName()));
            LOG.step("9.2 Change the value of 'Use Cycle Partitioning for Data Directory' to false and 'Use Cycle Partitioning for Summary Directories' to false, then check that 'List Individual Files in Contents' is true");
            taskView = changeAdditionalCheckboxesForTask3(false, false, task3);
            softAssert.assertTrue(Boolean.parseBoolean(taskView.getTargetValue(TaskLabels.LIST_INDIVIDUAL_FILES_IN_CONTENTS)), String.format("Wrong List Individual Files in Contents value. ", task3.getName()));
            LOG.step("9.3 Change the value of 'Use Cycle Partitioning for Data Directory' to true and 'Use Cycle Partitioning for Summary Directories' to true, then check that 'List Individual Files in Contents' is false");
            taskView = changeAdditionalCheckboxesForTask3(true, true, task3);
            softAssert.assertFalse(Boolean.parseBoolean(taskView.getTargetValue(TaskLabels.LIST_INDIVIDUAL_FILES_IN_CONTENTS)), String.format("Wrong List Individual Files in Contents value. ", task3.getName()));
        }

        softAssert.assertAll();
    }

    private DataIngestionWizardStep3 addAdvancedCheckBox(Task task) {
        SoftAssert softAssert = new SoftAssert();
        step3 = step1.next().next();
        //Fill required fields in case of Incremental Load S3/ADLSGEN2/Google task
        if (task.isCDC() && new Connection(targetConnection).isFileStorage()) {
            step3.fillTargetDirectory("targetDirectory");
            DataIngestionWizardStep4 step4 = step3.next();
            step4.fillApplyCycleChangeLimit(1);
            step3 = step4.back();
        }
        step3.addAdvancedCheckBox();
        task.setAddOperationType(true);
        task.setAddOperationTime(true);
        task.setAddOperationOwner(true);
        task.setAddOperationTransactionId(true);
        task.setAddBeforeImages(true);
        step3.save();
        softAssert.assertTrue(step3.isSuccess(), "Success alert was not shown on creation task with Advanced CheckBoxes");
        softAssert.assertAll();

        return step3;
    }

    private DataIngestionTaskView changeAdditionalCheckboxesForTask1(boolean condition, Task task) {
        taskView.editTask();
        if (!step3.getUseCyclePartitioningForDataDirectory().exists()) {
            step1.next().next();
        }
        step3.setUseCyclePartitioningForDataDirectory(condition);
        task.setUseCyclePartitioningForDataDirectory(condition);
        step3.save();
        return step3.view();
    }

    private DataIngestionTaskView changeAdditionalCheckboxesForTask2(boolean condition, boolean condition2, Task task) {
        taskView.editTask();
        if (!step3.getUseCyclePartitioningForDataDirectory().exists()) {
            step1.next().next();
        }
        step3.setUseCyclePartitioningForSummaryDirectories(condition);
        task.setUseCyclePartitioningForSummaryDirectories(condition);
        step3.setListIndividualFilesInContents(condition2);
        task.setListIndividualFilesInContents(condition2);
        step3.save();
        return step3.view();
    }

    private DataIngestionTaskView changeAdditionalCheckboxesForTask3(boolean condition, boolean condition2, Task task) {
        taskView.editTask();
        if (!step3.getUseCyclePartitioningForDataDirectory().exists()) {
            step1.next().next();
        }
        step3.setUseCyclePartitioningForDataDirectory(condition);
        task.setUseCyclePartitioningForDataDirectory(condition);
        step3.setUseCyclePartitioningForSummaryDirectories(condition2);
        task.setUseCyclePartitioningForSummaryDirectories(condition2);
        step3.save();
        return step3.view();
    }

}