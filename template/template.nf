#! /usr/bin/env nextflow
/*
 * LICENSE INFO
 */ 

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    ================================================================
    
    ================================================================
    """.stripIndent()
}

params.help = false
if (params.help) {
    helpMessage()
    exit 0
}


params.defdir = "$baseDir"
/*
 *  Defines some parameters in order to specify input 
 *  and advance argument by using the command line options
 */

/*>>>>>[PARAMS DEFINE SECTION]*/ 

// import modules
/*>>>>>[IMPORT MODULES SECTION]*/ 


workflow {
    /*
    *
    */
    
    // compose workflow
/*>>>>>[COMPOSE WORFLOW ]*/ 
    
}

workflow.onComplete { 
    log.info """\n\n
        [Atomic pipeline - BioColab DevTeam]
        Pipeline has finished.
        Status:   ${workflow.success ?  "Done!" : "Oops .. something went wrong"}
        Time:     ${workflow.complete}
        Duration: ${workflow.duration}\n
        """
        .stripIndent()
}