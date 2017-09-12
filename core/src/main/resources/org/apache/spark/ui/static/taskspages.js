/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

$(document).ajaxStop($.unblockUI);
$(document).ajaxStart(function () {
    $.blockUI({message: '<h3>Loading Tasks Page...</h3>'});
});

$.extend( $.fn.dataTable.ext.type.order, {
    "file-size-pre": ConvertDurationString,

    "file-size-asc": function ( a, b ) {
        a = ConvertDurationString( a );
        b = ConvertDurationString( b );
        return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },

    "file-size-desc": function ( a, b ) {
        a = ConvertDurationString( a );
        b = ConvertDurationString( b );
        return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    }
} );

function createTemplateURI(appId) {
    var words = document.baseURI.split('/');
    var ind = words.indexOf("proxy");
    if (ind > 0) {
        var baseURI = words.slice(0, ind + 1).join('/') + '/' + appId + '/static/stagespage-template.html';
        return baseURI;
    }
    ind = words.indexOf("history");
    if(ind > 0) {
        var baseURI = words.slice(0, ind).join('/') + '/static/stagespage-template.html';
        return baseURI;
    }
    return location.origin + "/static/stagespage-template.html";
}

// This function will only parse the URL under certain formate
// e.g. https://axonitered-jt1.red.ygrid.yahoo.com:50509/history/application_1502220952225_59143/stages/stage/?id=0&attempt=0
function StageEndPoint(appId) {
    var words = document.baseURI.split('/');
    var ind = words.indexOf("proxy");
    if (ind > 0) {
        var appId = words[ind + 1];
        var stageIdLen = words[ind + 3].indexOf('&');
        var stageId = words[ind + 3].substr(9, stageIdLen - 9);
        var newBaseURI = words.slice(0, ind + 2).join('/');
        return newBaseURI + "/api/v1/applications/" + appId + "/stages/" + stageId;
    }
    ind = words.indexOf("history");
    if (ind > 0) {
        var appId = words[ind + 1];
        var attemptId = words[ind + 4].split("&attempt=").pop();
        var stageIdLen = words[ind + 4].indexOf('&');
        var stageId = words[ind + 4].substr(4, stageIdLen - 4);
        var newBaseURI = words.slice(0, ind).join('/');
        if (isNaN(attemptId) || attemptId == "0") {
            return newBaseURI + "/api/v1/applications/" + appId + "/stages/" + stageId;
        } else {
            return newBaseURI + "/api/v1/applications/" + appId + "/" + attemptId + "/stages/" + stageId;
        }
    }
    var stageIdLen = words[ind + 3].indexOf('&');
    var stageId = words[ind + 3].substr(9, stageIdLen - 9);
    return location.origin + "/api/v1/applications/" + appId + "/stages/" + stageId;
}

function sortNumber(a,b) {
    return a - b;
}

function quantile(array, percentile) {
    index = percentile/100. * (array.length-1);
    if (Math.floor(index) == index) {
    	result = array[index];
    } else {
        var i = Math.floor(index);
        fraction = index - i;
        result = array[i];
    }
    return result;
}

$(document).ready(function () {
    $.extend($.fn.dataTable.defaults, {
        stateSave: true,
        lengthMenu: [[20, 40, 60, 100, -1], [20, 40, 60, 100, "All"]],
        pageLength: 20
    });

    $("#showAdditionalMetrics").append(
        "<div><a id='taskMetric'>" +
        "<span class='expand-input-rate-arrow arrow-closed' id='arrowtoggle1'></span>" +
        " Show Additional Metrics" +
        "</a></div>" +
        "<div class='container-fluid' id='toggle-metrics' hidden>" +
        "<div><input type='checkbox' class='toggle-vis' data-column='0'> Select All</div>" +
        "<div><input type='checkbox' class='toggle-vis' data-column='10'> Scheduler Delay</div>" +
        "<div><input type='checkbox' class='toggle-vis' data-column='11'> Task Deserialization Time</div>" +
        "<div><input type='checkbox' class='toggle-vis' data-column='12'> Result Serialization Time</div>" +
        "<div><input type='checkbox' class='toggle-vis' data-column='13'> Getting Result Time</div>" +
        "<div><input type='checkbox' class='toggle-vis' data-column='14'> Peak Execution Memory</div>" +
        "</div>");

    tasksSummary = $("#active-tasks");
    getStandAloneAppId(function (appId) {

        var endPoint = StageEndPoint(appId);
        $.getJSON(endPoint, function(response, status, jqXHR) {

            // prepare data for tasks table
            var indices = Object.keys(response[0].tasks);
            var task_table = [];
            indices.forEach(function (ix){
               task_table.push(response[0].tasks[ix]);
            });

            // prepare data for task aggregated metrics table
            indices = Object.keys(response[0].executorSummary);
            var task_summary_table = [];
            indices.forEach(function (ix){
               response[0].executorSummary[ix].id = ix;
               task_summary_table.push(response[0].executorSummary[ix]);
            });

            // prepare data for task summary table
            var durationSummary = [];
            var schedulerDelaySummary = [];
            var taskDeserializationSummary = [];
            var gcTimeSummary = [];
            var resultSerializationTimeSummary = [];
            var gettingResultTimeSummary = [];
            var peakExecutionMemorySummary = [];

            task_table.forEach(function (x){
                durationSummary.push(x.taskMetrics.executorRunTime);
                schedulerDelaySummary.push(x.schedulerDelay);
                taskDeserializationSummary.push(x.taskMetrics.executorDeserializeTime);
                gcTimeSummary.push(x.taskMetrics.jvmGcTime);
                resultSerializationTimeSummary.push(x.taskMetrics.resultSerializationTime);
                gettingResultTimeSummary.push(x.gettingResultTime);
                peakExecutionMemorySummary.push(x.taskMetrics.peakExecutionMemory);
            });

            var task_metrics_table = [];
            var task_metrics_table_all = [];
            var task_metrics_table_col = ["Duration", "Scheduler Delay", "Task Deserialization Time", "GC Time", "Result Serialization Time", "Getting Result Time", "Peak Execution Memory"];

            task_metrics_table_all.push(durationSummary);
            task_metrics_table_all.push(schedulerDelaySummary);
            task_metrics_table_all.push(taskDeserializationSummary);
            task_metrics_table_all.push(gcTimeSummary);
            task_metrics_table_all.push(resultSerializationTimeSummary);
            task_metrics_table_all.push(gettingResultTimeSummary);
            task_metrics_table_all.push(peakExecutionMemorySummary);

            for(i = 0; i < task_metrics_table_col.length; i++){
                var task_sort_table = (task_metrics_table_all[i]).sort(sortNumber);
                var row = {
                    "metric": task_metrics_table_col[i],
                    "p0": quantile(task_sort_table, 0),
                    "p25": quantile(task_sort_table, 25),
                    "p50": quantile(task_sort_table, 50),
                    "p75": quantile(task_sort_table, 75),
                    "p100": quantile(task_sort_table, 100)
                };
                task_metrics_table.push(row);
            }

            // prepare data for accumulatorUpdates
            var indices = Object.keys(response[0].accumulatorUpdates);
            var accumulator_table_all = [];
            var accumulator_table = [];
            indices.forEach(function (ix){
               accumulator_table_all.push(response[0].accumulatorUpdates[ix]);
            });

            accumulator_table_all.forEach(function (x){
                var name = (x.name).toString();
                if(name.includes("internal.") == false){
                    accumulator_table.push(x);
                }
            });

            // rendering the UI page
            var data = {executors: response, "taskstable": task_table, "task_metrics_table": task_metrics_table};
            $.get(createTemplateURI(appId), function(template) {
                tasksSummary.append(Mustache.render($(template).filter("#stages-summary-template").html(), data));

                $("#taskMetric").click(function(){
                    $("#arrowtoggle1").toggleClass("arrow-open arrow-closed");
                    $("#toggle-metrics").toggle();
                });

                $("#aggregatedMetrics").click(function(){
                    $("#arrowtoggle2").toggleClass("arrow-open arrow-closed");
                    $("#toggle-aggregatedMetrics").toggle();
                });

                // building task summary table
                var taskMetricsTable = "#summary-metrics-table";
                var task_conf = {
                    "data": task_metrics_table,
                    "columns": [
                        {data : 'metric'},
                        {
                            data: function (row, type) {
                                return row.metric != 'Peak Execution Memory' ? (formatDuration(row.p0)) : formatBytes(row.p0, type);
                            }
                        },
                        {
                            data: function (row, type) {
                                return row.metric != 'Peak Execution Memory' ? (formatDuration(row.p25)) : formatBytes(row.p25, type);
                            }
                        },
                        {
                            data: function (row, type) {
                                return row.metric != 'Peak Execution Memory' ? (formatDuration(row.p50)) : formatBytes(row.p50, type);
                            }
                        },
                        {
                            data: function (row, type) {
                                return row.metric != 'Peak Execution Memory' ? (formatDuration(row.p75)) : formatBytes(row.p75, type);
                            }
                        },
                        {
                            data: function (row, type) {
                                return row.metric != 'Peak Execution Memory' ? (formatDuration(row.p100)) : formatBytes(row.p100, type);
                            }
                        }
                    ],
                    "columnDefs": [
                        { "type": "file-size", "targets": 1 },
                        { "type": "file-size", "targets": 2 },
                        { "type": "file-size", "targets": 3 },
                        { "type": "file-size", "targets": 4 },
                        { "type": "file-size", "targets": 5 }
                    ],
                    "paging": false,
                    "searching": false,
                    "order": [[0, "asc"]]
                };
                $(taskMetricsTable).DataTable(task_conf);

               // building task aggregated metric table
                var tasksSummarytable = "#summary-stages-table";
                var task_summary_conf = {
                    "data": task_summary_table,
                    "columns": [
                        {data : "id"},
                        {data : "executorLogs", render: formatLogsCells},
                        {data : "host"},
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskTime) : row.taskTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                var totaltasks = row.succeededTasks + row.failedTasks + row.killedTasks;
                                return type === 'display' ? totaltasks : totaltasks.toString();
                            }
                        },
                        {data : "failedTasks"},
                        {data : "killedTasks"},
                        {data : "succeededTasks"},
                        {data : "blacklisted"},
                        {
                            data : function (row, type) {
                                return row.inputRecords != 0 ? formatBytes(row.inputBytes/row.inputRecords) : 0;
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.outputRecords != 0 ? formatBytes(row.outputBytes/row.outputRecords) : 0;
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.shuffleReadRecords != 0 ? formatBytes(row.shuffleRead/row.shuffleReadRecords) : 0;
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.shuffleWriteRecords != 0 ? formatBytes(row.shuffleWrite/row.shuffleWriteRecords) : 0;
                            }
                        },
                        {
                            data : function (row, type) {
                                return typeof row.memoryBytesSpilled != 'undefined' ? formatBytes(row.memoryBytesSpilled) : "";
                            }
                        },
                        {
                            data : function (row, type) {
                                return typeof row.diskBytesSpilled != 'undefined' ? formatBytes(row.diskBytesSpilled) : "";
                            }
                        }
                    ],
                    "order": [[0, "asc"]]
                }
                $(tasksSummarytable).DataTable(task_summary_conf);
                $('#active-tasks [data-toggle="tooltip"]').tooltip();

                // building accumulator update table
                var accumulatorTable = "#accumulator-table";
                var accumulator_conf = {
                    "data": accumulator_table,
                    "columns": [
                        {data : "id"},
                        {data : "name"},
                        {data : "value"}
                    ],
                    "paging": false,
                    "searching": false,
                    "order": [[0, "asc"]]
                }
                $(accumulatorTable).DataTable(accumulator_conf);

                // building tasks table
                var taskTable = "#active-tasks-table";
                var task_conf = {
                    "data": task_table,
                    "columns": [
                        {data: function (row, type) {
                            return type !== 'display' ? (isNaN(row.index) ? 0 : row.index ) : row.index;
                            }
                        },
                        {data : "taskId"},
                        {data : "attempt"},
                        {data : "taskState"},
                        {data : "taskLocality"},
                        {
                            data : function (row, type) {
                                return row.executorId + ' / ' + row.host;
                            }
                        },
                        {data : "executorLogs", render: formatLogsCells},
                        {data : "launchTime"},
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskMetrics.executorRunTime) : row.taskMetrics.executorRunTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskMetrics.jvmGcTime) : row.taskMetrics.jvmGcTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.schedulerDelay) : row.schedulerDelay;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskMetrics.executorDeserializeTime) : row.taskMetrics.executorDeserializeTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskMetrics.resultSerializationTime) : row.taskMetrics.resultSerializationTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.gettingResultTime) : row.gettingResultTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskMetrics.peakExecutionMemory) : row.taskMetrics.peakExecutionMemory;
                            }
                        },
                        {
                            data : function (row, type) {
                                var msg = row.errorMessage;
                                if (typeof msg === 'undefined'){
                                    return "";
                                } else {
                                        var form_head = msg.substring(0, msg.indexOf("at"));
                                        var form = "<span onclick=\"this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')\" class=\"expand-details\">+details</span>";
                                        var form_msg = "<div class=\"stacktrace-details collapsed\"><pre>" + row.errorMessage + "</pre></div>";
                                        return form_head + form + form_msg;
                                }
                            }
                        }
                    ],
                    "columnDefs": [
                        { "visible": false, "targets": 10 },
                        { "visible": false, "targets": 11 },
                        { "visible": false, "targets": 12 },
                        { "visible": false, "targets": 13 },
                        { "visible": false, "targets": 14 }
                    ],
                    "order": [[0, "asc"]]
                };
                var taskTableSelector = $(taskTable).DataTable(task_conf);

                // hide or show columns dynamically event
                $('input.toggle-vis').on('click', function(e){
                    // Get the column
                    var para = $(this).attr('data-column');
                    if(para == "0"){
                        var column = taskTableSelector.column([10, 11, 12, 13, 14]);
                        if($(this).is(":checked")){
                            $(".toggle-vis").prop('checked', true);
                            column.visible(true);
                        } else {
                            $(".toggle-vis").prop('checked', false);
                            column.visible(false);
                        }
                    } else {
                    var column = taskTableSelector.column($(this).attr('data-column'));
                    // Toggle the visibility
                    column.visible(!column.visible());
                    }
                });

                // title number and toggle list
                $("#summaryMetricsTitle").html("Summary Metrics for " + task_table.length + " Completed Tasks");
                $("#tasksTitle").html("Task (" + task_table.length + ")");

                // hide or show the accumulate update table
                if(accumulator_table.length == 0){
                    $("accumulator-update-table").hide();
                }
            });
        });
    });
});
