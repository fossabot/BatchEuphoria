/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.pbs

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.StringConstants
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.execution.jobs.cluster.ClusterJobManager
import de.dkfz.roddy.tools.*
import groovy.util.slurpersupport.GPathResult

import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.regex.Matcher

/**
 * A job submission implementation for standard PBS systems.
 *
 * @author michael
 */
@groovy.transform.CompileStatic
class PBSJobManager extends ClusterJobManager<PBSCommand> {

    private static final String PBS_COMMAND_QUERY_STATES = "qstat -t"
    private static final String PBS_COMMAND_QUERY_STATES_FULL = "qstat -x -f "
    private static final String PBS_COMMAND_DELETE_JOBS = "qdel"
    private static final String WITH_DELIMITER = '(?=(%1$s))'

    PBSJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
        /**
         * General or specific todos for BatchEuphoriaJobManager and PBSJobManager
         */
    }

    @Override
    protected PBSCommand createCommand(BEJob job) {
        return new PBSCommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript())
    }

    /**
     * For PBS, we enable hold jobs by default.
     * If it is not enabled, we might run into the problem, that job dependencies cannot be
     * resolved early enough due to timing problems.
     * @return
     */
    @Override
    boolean getDefaultForHoldJobsEnabled() { return true }

    @Override
    protected ExecutionResult executeStartHeldJobs(List<BEJobID> jobIDs) {
        String command = "qrls ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    GenericJobInfo parseGenericJobInfo(String commandString) {
        return new PBSCommandParser(commandString).toGenericJobInfo();
    }

    private String getQueryCommand() {
        return PBS_COMMAND_QUERY_STATES
    }

    @Override
    protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs) {
        StringBuilder queryCommand = new StringBuilder(getQueryCommand())

        if (jobIDs && jobIDs.size() < 10) {
            queryCommand << " "<< jobIDs*.id.join(" ")
        }

        if (isTrackingOfUserJobsEnabled)
            queryCommand << " -u $userIDForQueries "

        ExecutionResult er = executionService.execute(queryCommand.toString())
        List<String> resultLines = er.resultLines

        Map<BEJobID, JobState> result = [:]

        if (!er.successful) {
            if (strictMode) // Do not pull this into the outer if! The else branch needs to be executed if er.successful is true
                throw new BEException("The execution of ${queryCommand} failed.", null)
        } else {
            if (resultLines.size() > 2) {

                for (String line : resultLines) {
                    line = line.trim()
                    if (line.length() == 0) continue
                    if (!RoddyConversionHelperMethods.isInteger(line.substring(0, 1)))
                        continue //Filter out lines which have been missed which do not start with a number.

                    //TODO Put to a common class, is used multiple times.
                    line = line.replaceAll("\\s+", " ").trim()       //Replace multi white space with single whitespace
                    String[] split = line.split(" ")
                    final int ID = getPositionOfJobID()
                    final int JOBSTATE = getPositionOfJobState()

                    BEJobID jobID = new BEJobID(split[ID])

                    JobState js = parseJobState(split[JOBSTATE])
                    result.put(jobID, js)
                }
            }
        }
        return result
    }

    @Override
    String getJobIdVariable() {
        return "PBS_JOBID"
    }

    @Override
    String getJobNameVariable() {
        return "PBS_JOBNAME"
    }

    @Override
    String getQueueVariable() {
        return 'PBS_QUEUE'
    }

    @Override
    String getNodeFileVariable() {
        return "PBS_NODEFILE"
    }

    @Override
    String getSubmitHostVariable() {
        return "PBS_O_HOST"
    }

    @Override
    String getSubmitDirectoryVariable() {
        return "PBS_O_WORKDIR"
    }

    protected int getPositionOfJobID() {
        return 0
    }

    /**
     * Return the position of the status string within a stat result line. This changes if -u USERNAME is used!
     *
     * @return
     */
    protected int getPositionOfJobState() {
        if (isTrackingOfUserJobsEnabled)
            return 9
        return 4
    }

    @Override
    Map<BEJobID, GenericJobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates
        String qStatCommand = PBS_COMMAND_QUERY_STATES_FULL
        if (isTrackingOfUserJobsEnabled)
            qStatCommand += " -u $userIDForQueries "

        qStatCommand += jobIds.collect { it }.join(" ")

        ExecutionResult er = executionService.execute(qStatCommand.toString())

        if (er != null && er.successful) {
            queriedExtendedStates = this.processQstatOutput(er.resultLines.join("\n"))
        }else{
            throw new BEException("Extended job states couldn't be retrieved. \n Returned status code:${er.exitCode} \n result:${er.resultLines}")
        }
        return queriedExtendedStates
    }

    @Override
    ExecutionResult executeKillJobs(List<BEJobID> jobIDs) {
        String command = "qdel ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    String parseJobID(String commandOutput) {
        return commandOutput
    }

    @Override
    String getSubmissionCommand() {
        return "qsub"
    }

    /**
     * Reads qstat output
     * @param qstatOutput
     * @return output of qstat in a map with jobid as key
     */
    private Map<String, Map<String, String>> readQstatOutput(String qstatOutput) {
        return qstatOutput.split(String.format(WITH_DELIMITER, "\n\nJob Id: ")).collectEntries {
            Matcher matcher = it =~ /^\s*Job Id: (?<jobId>\d+)\..*\n/
            def result = new HashMap()
            if (matcher) {
                result[matcher.group("jobId")] = it
            }
            result
        }.collectEntries { jobId, value ->
            // join multi-line values
            value = ((String) value).replaceAll("\n\t", "")
            [(jobId): value]
        }.collectEntries { jobId, value ->
            Map<String, String> p = ((String) value).readLines().
                    findAll { it.startsWith("    ") && it.contains(" = ") }.
                    collectEntries {
                        String[] parts = it.split(" = ")
                        new MapEntry(parts.head().replaceAll(/^ {4}/, ""), parts.tail().join(' '))
                    }
            [(jobId): p]
        } as Map<String, Map<String, String>>
    }

    private static LocalDateTime parseTime(String str) {
        return catchAndLogExceptions {Instant.ofEpochSecond(Long.valueOf(str)).atZone(ZoneId.systemDefault()).toLocalDateTime()}
    }

    /**
     * Reads the qstat output and creates GenericJobInfo objects
     * @param resultLines - Input of ExecutionResult object
     * @return map with jobid as key
     */
    private Map<BEJobID, GenericJobInfo> processQstatOutput(String result) {
        Map<BEJobID, GenericJobInfo> queriedExtendedStates = [:]
        if (result.isEmpty()) {
            return [:]
        }

        GPathResult parsedJobs = new XmlSlurper().parseText(result)

        parsedJobs.children().each { Object job ->
            String jobIdRaw = job["Job_Id"] as String
            BEJobID jobID
            try{
                jobID = new BEJobID(jobIdRaw)
            }catch (Exception exp){
                throw new BEException("Job ID '${jobIdRaw}' could not be transformed to BEJobID ")
            }
            List<String> jobDependencies = job["depend"] ? (job["depend"] as  String).find("afterok.*")?.findAll(/(\d+).(\w+)/) { fullMatch, beforeDot, afterDot -> return beforeDot } : null
            GenericJobInfo gj = new GenericJobInfo(job["Job_Name"] as String ?: null, null, jobID, null, jobDependencies)

            BufferValue mem = null
            Integer cores
            Integer nodes
            TimeUnit walltime = null
            String additionalNodeFlag

            if (job["Resource_List"]["mem"])
                mem = catchAndLogExceptions { new BufferValue(Integer.valueOf((job["Resource_List"]["mem"] as String).find(/(\d+)/)), BufferUnit.valueOf((job["Resource_List"]["mem"] as String)[-2])) }
            if (job["Resource_List"]["nodect"])
                nodes = catchAndLogExceptions { Integer.valueOf(job["Resource_List"]["nodect"] as String) }
            if (job["Resource_List"]["nodes"])
                cores = catchAndLogExceptions { Integer.valueOf((job["Resource_List"]["nodes"] as String).find("ppn=.*").find(/(\d+)/)) }
            if (job["Resource_List"]["nodes"])
                additionalNodeFlag = catchAndLogExceptions { (job["Resource_List"]["nodes"] as String).find(/(\d+):(\.*)/) { fullMatch, nCores, feature -> return feature } }
            if (job["Resource_List"]["walltime"])
                walltime = catchAndLogExceptions { new TimeUnit(job["Resource_List"]["walltime"] as String) }

            BufferValue usedMem = null
            TimeUnit usedWalltime = null
            if (job["resources_used"]["mem"])
                catchAndLogExceptions { usedMem = new BufferValue(Integer.valueOf((job["resources_used"]["mem"] as String).find(/(\d+)/)), BufferUnit.valueOf((job["resources_used"]["mem"] as String)[-2]))  }
            if (job["resources_used"]["walltime"])
                catchAndLogExceptions { usedWalltime = new TimeUnit(job["resources_used"]["walltime"] as String) }


            gj.setAskedResources(new ResourceSet(null, mem, cores, nodes, walltime, null, job["queue"] as String ?: null, additionalNodeFlag))
            gj.setUsedResources(new ResourceSet(null, usedMem, null, null, usedWalltime, null, job["queue"] as String ?: null, null))

            gj.setLogFile(getQstatFile((job["Output_Path"] as String).replace("\$PBS_JOBID", jobIdRaw)))
            gj.setErrorLogFile(getQstatFile((job["Error_Path"] as String).replace("\$PBS_JOBID", jobIdRaw)))
            gj.setUser(job["euser"] as String ?: null)
            gj.setExecutionHosts(job["exec_host"] as String ? [job["exec_host"] as String] : null)
            gj.setSubmissionHost(job["submit_host"] as String ?: null)
            gj.setPriority(job["Priority"] as String ?: null)
            gj.setUserGroup(job["egroup"] as String ?: null)
            gj.setResourceReq(job["submit_args"] as String ?: null)
            gj.setRunTime(job["total_runtime"] ? catchAndLogExceptions { Duration.ofSeconds(Math.round(Double.parseDouble(job["total_runtime"] as String)), 0) } : null)
            gj.setCpuTime(job["resources_used"]["cput"] ? catchAndLogExceptions { parseColonSeparatedHHMMSSDuration(job["resources_used"]["cput"] as String) } : null)
            gj.setServer(job["server"] as String ?: null)
            gj.setUmask(job["umask"] as String ?: null)
            gj.setJobState(parseJobState(job["job_state"] as String))
            gj.setExitCode(job["exit_status"] ? catchAndLogExceptions { Integer.valueOf(job["exit_status"] as String) }: null )
            gj.setAccount(job["Account_Name"] as String ?: null)
            gj.setStartCount(job["start_count"] ? catchAndLogExceptions { Integer.valueOf(job["start_count"] as String) } : null)

            if (job["qtime"]) // The time that the job entered the current queue.
                gj.setSubmitTime(parseTime(job["qtime"] as String))
            if (job["start_time"]) // The timepoint the job was started.
                gj.setStartTime(parseTime(job["start_time"] as String))
            if (job["comp_time"])  // The timepoint the job was completed.
                gj.setEndTime(parseTime(job["comp_time"] as String))
            if (job["etime"])  // The time that the job became eligible to run, i.e. in a queued state while residing in an execution queue.
                gj.setEligibleTime(parseTime(job["etime"] as String))

            queriedExtendedStates.put(jobID, gj)
        }
        return queriedExtendedStates
    }

    private static File getQstatFile(String s) {
        if (!s) {
            return null
        } else if (s.startsWith("/")) {
            return new File(s)
        } else if (s =~ /^[\w-]+:\//) {
            return new File(s.replaceAll(/^[\w-]+:/, ""))
        } else {
            return null
        }
    }

    protected JobState parseJobState(String stateString) {
        // http://docs.adaptivecomputing.com/torque/6-1-0/adminGuide/help.htm#topics/torque/commands/qstat.htm
        JobState js = JobState.UNKNOWN
        if (stateString == "R")
            js = JobState.RUNNING
        if (stateString == "H")
            js = JobState.HOLD
        if (stateString == "S")
            js = JobState.SUSPENDED
        if (stateString in ["Q", "T", "W"])
            js = JobState.QUEUED
        if (stateString in ["C", "E"]) {
            js = JobState.COMPLETED_UNKNOWN
        }

        return js;
    }

    @Override
    List<String> getEnvironmentVariableGlobs() {
        return Collections.unmodifiableList(["PBS_*"])
    }

    void createDefaultManagerParameters(LinkedHashMultimap<String, String> parameters) {

    }

    void createComputeParameter(ResourceSet resourceSet, LinkedHashMultimap<String, String> parameters) {
        int nodes = resourceSet.isNodesSet() ? resourceSet.getNodes() : 1
        int cores = resourceSet.isCoresSet() ? resourceSet.getCores() : 1
        // Currently not active
        String enforceSubmissionNodes = ''
        if (!enforceSubmissionNodes) {
            String pVal = 'nodes=' << nodes << ':ppn=' << cores
            if (resourceSet.isAdditionalNodeFlagSet()) {
                pVal << ':' << resourceSet.getAdditionalNodeFlag()
            }
            parameters.put("-l", pVal)
        } else {
            String[] nodesArr = enforceSubmissionNodes.split(StringConstants.SPLIT_SEMICOLON)
            nodesArr.each {
                String node ->
                    parameters.put('-l', 'nodes=' + node + ':ppn=' + resourceSet.getCores())
            }
        }
    }

    /**
     * Valid for GE, PBS, LSF
     * @param parameters
     * @param queue
     * @return
     */
    void createQueueParameter(LinkedHashMultimap<String, String> parameters, String queue) {
        parameters.put('-q', queue)
    }

    /**
     * Valid only for PBS
     * @param parameters
     * @param resourceSet
     * @return
     */
    void createWalltimeParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put('-l', 'walltime=' + TimeUnit.fromDuration(resourceSet.walltime))
    }

    void createMemoryParameter(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
        parameters.put('-l', 'mem=' + resourceSet.getMem().toString(BufferUnit.M))
    }

    void createStorageParameters(LinkedHashMultimap<String, String> parameters, ResourceSet resourceSet) {
    }
}
