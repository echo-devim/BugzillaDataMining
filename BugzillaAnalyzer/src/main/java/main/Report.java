package main;

import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;

import scala.Tuple2;

public class Report {

	public static String generateHTML(long tot_bugs, long datasets, long bugs_fixed, long bugs_closed, long bugs_open,
			long bugs_invalid, long security_bugs, long io_bugs, long memory_bugs, long network_bugs,
			List<Tuple2<String,Long>> bugs_per_function, List<Tuple2<String,Long>> bugs_per_year, List<Tuple2<String,Long>> bugs_per_hour,
			List<Tuple2<String,Long>> bugs_per_program, List<Tuple2<String,Long>> bugs_per_component, List<Tuple2<String,Long>> bugs_languages,
			long italian_users, List<Tuple2<String,Long>> users_tot_bugs, List<Tuple2<String,Long>> users_tot_datasets) {

		DecimalFormat df = new DecimalFormat("#.##");
		
		String html = "<!doctype html><html><head>    <title>Report</title>    <script src=\"https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.1.6/Chart.bundle.js\"></script>    <script src=\"http://cdnjs.cloudflare.com/ajax/libs/jquery/2.1.3/jquery.min.js\"></script>    <style>    .spacer {    height: 50px;    }    html,body {    padding: 0;    margin: 0;    }table {border: 1px solid #80bfff;    border-collapse: collapse;    width: 100%;    height: 180px; }th, td {    text-align: left;    padding: 8px;}tr:nth-child(even){background-color: #e8e8ff}.title {font-size: 20pt;padding: 20px;}hr { style=\"margin-top: 3%; margin-bottom: 3%; color: #00134d\";}</style></head><body><div style=\"background-color: #00134d; color: #ffffff; font-size: 40pt; width: 100%; text-align:center; padding: 10px; margin-bottom: 20px;\">"
				+ tot_bugs + " Bugs in " + datasets
				+ " Datasets</div><div>    <div id=\"canvas-holder\" style=\"float: right; width:50%\">    <div class=\"title\">Bugs Status</div>        <canvas id=\"bug-status-chart\" />    </div>    <div id=\"canvas-holder\" style=\"float: left; width:50%\">    <div class=\"title\">Bugs Type</div>        <canvas id=\"bug-type-chart\" />    </div>    </div>    <div>        <div style=\"float: right; width: 30%; margin-top: 3%; margin-right: 10%\">        <table>  <tr>    <td>Open</td>    <td>"
				+ df.format(((bugs_open * 100.) / tot_bugs)) + "%</td>  </tr>  <tr>    <td>Closed</td>    <td>"
				+ df.format(((bugs_closed * 100.) / tot_bugs)) + "%</td>  </tr>  <tr>    <td>Fixed</td>    <td>"
				+ df.format(((bugs_fixed * 100.) / bugs_closed)) + "%</td>  </tr>  <tr>      <td>Invalid</td>      <td>"
				+ df.format(((bugs_invalid * 100.) / bugs_closed))
				+ "%</td>  </tr></table>    </div>    <div style=\"float: left; width: 30%; margin-top: 3%; margin-left: 10%\">        <table>  <tr>    <td>Security</td>    <td>"
				+ df.format(((security_bugs * 100.) / tot_bugs)) + "%</td>  </tr>  <tr>    <td>Memory</td>    <td>"
				+ df.format(((memory_bugs * 100.) / tot_bugs)) + "%</td>  </tr>  <tr>    <td>I/O</td>    <td>"
				+ df.format(((io_bugs * 100.) / tot_bugs)) + "%</td>  </tr>  <tr>    <td>Network</td>    <td>"
				+ df.format(((network_bugs * 100.) / tot_bugs)) + "%</td>  </tr>  <tr>    <td>Others</td>    <td>"
				+ df.format((((tot_bugs - security_bugs - memory_bugs - io_bugs - network_bugs) * 100.) / tot_bugs))
				+ "%</td>  </tr></table>    </div>    </div>    <div style=\"clear: both;\"></div>    <hr>    <div style=\"width: 100%; text-align:center;\"> <!-- Time stats -->    <div class=\"title\">Bugs per year</div>    <div id=\"canvas-holder\" style=\"margin: auto; width:60%\">    <canvas id=\"bug-per-year-chart\" /></div><div class=\"spacer\"></div><div class=\"title\">Bugs per hour</div>    <div id=\"canvas-holder\" style=\"margin: auto; width:60%\">    <canvas id=\"bug-per-hour-chart\" /></div><hr> <!-- Code stats --><b><div class=\"title\">Top 10</div></b><div class=\"title\">Bugs per function</div>    <div id=\"canvas-holder\" style=\"margin: auto; width:60%\">    <canvas id=\"bug-per-function-chart\" /></div><div class=\"title\">Bugs per program</div>    <div id=\"canvas-holder\" style=\"margin: auto; width:60%\">    <canvas id=\"bug-per-program-chart\" /></div><div class=\"title\">Bugs per component</div>    <div id=\"canvas-holder\" style=\"margin: auto; width:60%\">    <canvas id=\"bug-per-component-chart\" /></div><hr> <!-- Users stats --><div class=\"title\">Italian's users: <b>"
				+ df.format(((italian_users * 100.) / users_tot_bugs.size()))
				+ "%</b></div><b><div class=\"title\">Top 10</div></b><div class=\"title\">Cross-datasets users</div><div style=\"width: 30%;margin: auto;\"><table><tr><th>Username</th><th>Datasets</th></tr>";
		
		int i = 10;
		for (Tuple2<String,Long> t : users_tot_datasets) {
			if (t._2() > 1l)
				html += "<tr><td>" + t._1() + "</td><td>"+ t._2() + "</td></tr>";
			if (i == 0)
				break;
			else
				i--;
		}
		
		html += "</table></div><div class=\"title\">Bugs per user</div><div id=\"canvas-holder\" style=\"margin: auto; width:60%\">    <canvas id=\"bug-per-user-chart\" /></div><hr><div class=\"title\">Reports' languages</div><div style=\"width: 30%; margin: auto;\"><table>";

		i = 10;
		for (Tuple2<String,Long> t : bugs_languages) {
			html += "<tr><td>"+t._1()+"</td><td>"+t._2()+"</tr>";
			if (i == 0)
				break;
			else
				i--;
		}

		html += "</table></div></div>    <script>    var configBugStatus = {        type: 'doughnut',        data: {            datasets: [{                data: [ 0, "
				+ bugs_closed + ", " + bugs_open + ", 0],                backgroundColor: [                    \"#00cc00\",                    \"#0099ff\",                    \"#ffff00\",                    \"#ff9900\",                    \"#888888\"                ],            },{                data: ["
				+ bugs_fixed + ",0,0," + bugs_invalid + "],                backgroundColor: [                    \"#00cc00\",                    \"#0099ff\",                    \"#ffff00\",                    \"#ff9900\",                    \"#888888\"                ],            }],            labels: [                \"Fixed\",                \"Closed\",                \"Open\",                \"Invalid\"            ]        },        options: {            responsive: true        }    };    var configBugType = {        type: 'pie',        data: {            datasets: [{                data: [                    "
				+ security_bugs + ",                    " + memory_bugs + ",                    " + io_bugs
				+ ",                    " + network_bugs + ",  "
				+ (tot_bugs - security_bugs - memory_bugs - io_bugs - network_bugs)
				+ "  ],                backgroundColor: [                    \"#ffc61a\",                    \"#cc0044\",                    \"#80bfff\",                    \"#40bf80\",                    \"#888888\"                ],            }],            labels: [                \"Security\",                \"Memory\",                \"I/O\",                \"Network\",                \"Others\"            ]        },        options: {            responsive: true        }    };    var configBugPerYear = {    type: 'line',    data: {    labels: [";

		for (Tuple2<String,Long> t : bugs_per_year) {
			html += "\"" + t._1() + "\",";
		}
		html += "],    datasets: [        {            label: \"\",            fill: true,            lineTension: 0.1,            backgroundColor: \"rgba(75,192,192,0.4)\",            borderColor: \"rgba(75,192,192,1)\",            borderCapStyle: 'butt',            borderDash: [],            borderDashOffset: 0.0,            borderJoinStyle: 'miter',            pointBorderColor: \"rgba(75,192,192,1)\",            pointBackgroundColor: \"#fff\",            pointBorderWidth: 1,            pointHoverRadius: 5,            pointHoverBackgroundColor: \"rgba(75,192,192,1)\",            pointHoverBorderColor: \"rgba(220,220,220,1)\",            pointHoverBorderWidth: 2,            pointRadius: 1,            pointHitRadius: 10,            data: [";

		for (Tuple2<String,Long> t : bugs_per_year) {
			html += t._2() + ",";
		}
		html += "],        }    ]},options: {}};    var configBugPerHour = {    type: 'line',    data: {    labels: [";

		for (Tuple2<String,Long> t : bugs_per_hour) {
			html += "\"" + t._1() + "\",";
		}
		html += "],    datasets: [        {            label: \"\",            fill: true,            lineTension: 0.1,            backgroundColor: \"rgba(29,192,99,0.4)\",            borderColor: \"rgba(29,192,99,1)\",            borderCapStyle: 'butt',            borderDash: [],            borderDashOffset: 0.0,            borderJoinStyle: 'miter',            pointBorderColor: \"rgba(75,192,192,1)\",            pointBackgroundColor: \"#fff\",            pointBorderWidth: 1,            pointHoverRadius: 5,            pointHoverBackgroundColor: \"rgba(29,192,99,1)\",            pointHoverBorderColor: \"rgba(220,220,220,1)\",            pointHoverBorderWidth: 2,            pointRadius: 1,            pointHitRadius: 10,            data: [";

		for (Tuple2<String,Long> t : bugs_per_hour) {
			html += t._2() + ",";
		}

		html += "],        }    ]},options: {}};    var configBugPerFunction = {    type: 'bar',    data: {    labels: [";

		i = 10;
		for (Tuple2<String,Long> t : bugs_per_function) {
			html += "\"" + t._1() + "\",";
			if (i == 0)
				break;
			else
				i--;
		}

		html += "],    datasets: [        {            label: \"\",            backgroundColor: \"rgba(255,99,132,0.2)\",            borderColor: \"rgba(255,99,132,1)\",            borderWidth: 1,            hoverBackgroundColor: \"rgba(255,99,132,0.4)\",            hoverBorderColor: \"rgba(255,99,132,1)\",            data: [";

		i = 10;
		for (Tuple2<String,Long> t : bugs_per_function) {
			html += t._2() + ",";
			if (i == 0)
				break;
			else
				i--;
		}

		html += "],        }    ]},options: {}};    var configBugPerProgram = {    type: 'bar',    data: {    labels: [";
		i = 10;
		for (Tuple2<String,Long> t : bugs_per_program) {
			html += "\"" + t._1() + "\",";
			if (i == 0)
				break;
			else
				i--;
		}

		html += "],    datasets: [        {            label: \"\",            backgroundColor: \"rgba(80,120,60,0.2)\",            borderColor: \"rgba(80,120,60,1)\",            borderWidth: 1,            hoverBackgroundColor: \"rgba(80,120,60,0.4)\",            hoverBorderColor: \"rgba(80,120,60,1)\",            data: [";
		i = 10;
		for (Tuple2<String,Long> t : bugs_per_program) {
			html += t._2() + ",";
			if (i == 0)
				break;
			else
				i--;
		}
		html += "],        }    ]},options: {}};    var configBugPerComponent = {    type: 'bar',    data: {    labels: [";
		i = 10;
		for (Tuple2<String,Long> t : bugs_per_component) {
			html += "\"" + t._1() + "\",";
			if (i == 0)
				break;
			else
				i--;
		}
		html += "],    datasets: [        {            label: \"\",            backgroundColor: \"rgba(100,99,255,0.2)\",            borderColor: \"rgba(100,99,255,1)\",            borderWidth: 1,            hoverBackgroundColor: \"rgba(100,99,255,0.4)\",            hoverBorderColor: \"rgba(100,99,255,1)\",            data: [";
		i = 10;
		for (Tuple2<String,Long> t : bugs_per_component) {
			html += t._2() + ",";
			if (i == 0)
				break;
			else
				i--;
		}

		html += "],        }    ]},options: {}};    var configBugPerUser = {    type: 'horizontalBar',    data: {    labels: [";
		i = 10;
		for (Tuple2<String,Long> t : users_tot_bugs) {
			html += "\"" + t._1() + "\",";
			if (i == 0)
				break;
			else
				i--;
		}

		html += "],    datasets: [        {            label: \"\",            backgroundColor: \"rgba(64,224,208,0.2)\",            borderColor: \"rgba(64,194,180,1)\",            borderWidth: 1,            hoverBackgroundColor: \"rgba(64,194,180,0.4)\",            hoverBorderColor: \"rgba(64,194,180,1)\",            data: [";

		i = 10;
		for (Tuple2<String,Long> t : users_tot_bugs) {
			html += t._2() + ",";
			if (i == 0)
				break;
			else
				i--;
		}
		html += "],        }    ]},options: {}};    window.onload = function() {        new Chart(document.getElementById(\"bug-status-chart\").getContext(\"2d\"), configBugStatus);        new Chart(document.getElementById(\"bug-type-chart\").getContext(\"2d\"), configBugType);        Chart.defaults.global.legend.display = false;        new Chart(document.getElementById(\"bug-per-year-chart\").getContext(\"2d\"), configBugPerYear);        new Chart(document.getElementById(\"bug-per-hour-chart\").getContext(\"2d\"), configBugPerHour);        new Chart(document.getElementById(\"bug-per-function-chart\").getContext(\"2d\"), configBugPerFunction);        new Chart(document.getElementById(\"bug-per-program-chart\").getContext(\"2d\"), configBugPerProgram);        new Chart(document.getElementById(\"bug-per-component-chart\").getContext(\"2d\"), configBugPerComponent);        new Chart(document.getElementById(\"bug-per-user-chart\").getContext(\"2d\"), configBugPerUser);    };    </script></body></html>";

		return html;
	}

}
